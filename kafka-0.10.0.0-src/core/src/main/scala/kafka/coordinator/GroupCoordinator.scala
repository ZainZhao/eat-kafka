/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.coordinator

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.{OffsetAndMetadata, OffsetMetadataAndError, TopicAndPartition}
import kafka.log.LogConfig
import kafka.message.UncompressedCodec
import kafka.server._
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{OffsetFetchResponse, JoinGroupRequest}

import scala.collection.{Map, Seq, immutable}

case class GroupConfig(groupMinSessionTimeoutMs: Int,
                       groupMaxSessionTimeoutMs: Int)

case class JoinGroupResult(members: Map[String, Array[Byte]],
                           memberId: String,
                           generationId: Int,
                           subProtocol: String,
                           leaderId: String,
                           errorCode: Short)

/**
 * GroupCoordinator handles general group membership and offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of
 * groups. Groups are assigned to coordinators based on their group names.
 */
class GroupCoordinator(val brokerId: Int,
                       val groupConfig: GroupConfig,
                       val offsetConfig: OffsetConfig,
                       val groupManager: GroupMetadataManager,
                       val heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
                       val joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
                       time: Time) extends Logging {
  type JoinCallback = JoinGroupResult => Unit
  type SyncCallback = (Array[Byte], Short) => Unit

  this.logIdent = "[GroupCoordinator " + brokerId + "]: "

  private val isActive = new AtomicBoolean(false)

  def offsetsTopicConfigs: Properties = {
    val props = new Properties
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.SegmentBytesProp, offsetConfig.offsetsTopicSegmentBytes.toString)
    props.put(LogConfig.CompressionTypeProp, UncompressedCodec.name)
    props
  }

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup() {
    info("Starting up.")
    isActive.set(true)
    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown() {
    info("Shutting down.")
    isActive.set(false)
    groupManager.shutdown()
    heartbeatPurgatory.shutdown()
    joinPurgatory.shutdown()
    info("Shutdown complete.")
  }

  def handleJoinGroup(groupId: String,
                      memberId: String,
                      clientId: String,
                      clientHost: String,
                      sessionTimeoutMs: Int,
                      protocolType: String,
                      protocols: List[(String, Array[Byte])],
                      responseCallback: JoinCallback) {
    if (!isActive.get) {
      responseCallback(joinError(memberId, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code))
    } else if (!validGroupId(groupId)) {
      responseCallback(joinError(memberId, Errors.INVALID_GROUP_ID.code))
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(joinError(memberId, Errors.NOT_COORDINATOR_FOR_GROUP.code))
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      responseCallback(joinError(memberId, Errors.GROUP_LOAD_IN_PROGRESS.code))
    } else if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
               sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) {
      responseCallback(joinError(memberId, Errors.INVALID_SESSION_TIMEOUT.code))
    } else {
      // only try to create the group if the group is not unknown AND
      // the member id is UNKNOWN, if member is specified but group does not
      // exist we should reject the request
      var group = groupManager.getGroup(groupId)
      if (group == null) {
        if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID) {
          responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))
        } else {
          group = groupManager.addGroup(new GroupMetadata(groupId, protocolType))
          doJoinGroup(group, memberId, clientId, clientHost, sessionTimeoutMs, protocolType, protocols, responseCallback)
        }
      } else {
        doJoinGroup(group, memberId, clientId, clientHost, sessionTimeoutMs, protocolType, protocols, responseCallback)
      }
    }
  }

  private def doJoinGroup(group: GroupMetadata,
                          memberId: String,
                          clientId: String,
                          clientHost: String,
                          sessionTimeoutMs: Int,
                          protocolType: String,
                          protocols: List[(String, Array[Byte])],
                          responseCallback: JoinCallback) {
    group synchronized {
      if (group.protocolType != protocolType || !group.supportsProtocols(protocols.map(_._1).toSet)) {
        // if the new member does not support the group protocol, reject it
        responseCallback(joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL.code))
      } else if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID && !group.has(memberId)) {
        // if the member trying to register with a un-recognized id, send the response to let
        // it reset its member id and retry
        responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))
      } else {
        group.currentState match {
          case Dead =>
            // if the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; this is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // joining without the specified member id,
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))

          case PreparingRebalance =>
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              addMemberAndRebalance(sessionTimeoutMs, clientId, clientHost, protocols, group, responseCallback)
            } else {
              val member = group.get(memberId)
              updateMemberAndRebalance(group, member, protocols, responseCallback)
            }

          case AwaitingSync =>
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              addMemberAndRebalance(sessionTimeoutMs, clientId, clientHost, protocols, group, responseCallback)
            } else {
              val member = group.get(memberId)
              if (member.matches(protocols)) {
                // member is joining with the same metadata (which could be because it failed to
                // receive the initial JoinGroup response), so just return current group information
                // for the current generation.
                responseCallback(JoinGroupResult(
                  members = if (memberId == group.leaderId) {
                    group.currentMemberMetadata
                  } else {
                    Map.empty
                  },
                  memberId = memberId,
                  generationId = group.generationId,
                  subProtocol = group.protocol,
                  leaderId = group.leaderId,
                  errorCode = Errors.NONE.code))
              } else {
                // member has changed metadata, so force a rebalance
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              }
            }

          case Stable =>
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              // if the member id is unknown, register the member to the group
              addMemberAndRebalance(sessionTimeoutMs, clientId, clientHost, protocols, group, responseCallback)
            } else {
              val member = group.get(memberId)
              if (memberId == group.leaderId || !member.matches(protocols)) {
                // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
                // The latter allows the leader to trigger rebalances for changes affecting assignment
                // which do not affect the member metadata (such as topic metadata changes for the consumer)
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              } else {
                // for followers with no actual change to their metadata, just return group information
                // for the current generation which will allow them to issue SyncGroup
                responseCallback(JoinGroupResult(
                  members = Map.empty,
                  memberId = memberId,
                  generationId = group.generationId,
                  subProtocol = group.protocol,
                  leaderId = group.leaderId,
                  errorCode = Errors.NONE.code))
              }
            }
        }

        if (group.is(PreparingRebalance))
          joinPurgatory.checkAndComplete(GroupKey(group.groupId))
      }
    }
  }

  def handleSyncGroup(groupId: String,
                      generation: Int,
                      memberId: String,
                      groupAssignment: Map[String, Array[Byte]],
                      responseCallback: SyncCallback) {
    if (!isActive.get) {
      responseCallback(Array.empty, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Array.empty, Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else {
      val group = groupManager.getGroup(groupId)
      if (group == null)
        responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)
      else
        doSyncGroup(group, generation, memberId, groupAssignment, responseCallback)
    }
  }

  private def doSyncGroup(group: GroupMetadata,
                          generationId: Int,
                          memberId: String,
                          groupAssignment: Map[String, Array[Byte]],
                          responseCallback: SyncCallback) {
    var delayedGroupStore: Option[DelayedStore] = None

    group synchronized {
      if (!group.has(memberId)) {
        responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)
      } else if (generationId != group.generationId) {
        responseCallback(Array.empty, Errors.ILLEGAL_GENERATION.code)
      } else {
        group.currentState match {
          case Dead =>
            responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)

          case PreparingRebalance =>
            responseCallback(Array.empty, Errors.REBALANCE_IN_PROGRESS.code)

          case AwaitingSync =>
            group.get(memberId).awaitingSyncCallback = responseCallback
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))

            // if this is the leader, then we can attempt to persist state and transition to stable
            if (memberId == group.leaderId) {
              info(s"Assignment received from leader for group ${group.groupId} for generation ${group.generationId}")

              // fill any missing members with an empty assignment
              val missing = group.allMembers -- groupAssignment.keySet
              val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

              delayedGroupStore = Some(groupManager.prepareStoreGroup(group, assignment, (errorCode: Short) => {
                group synchronized {
                  // another member may have joined the group while we were awaiting this callback,
                  // so we must ensure we are still in the AwaitingSync state and the same generation
                  // when it gets invoked. if we have transitioned to another state, then do nothing
                  if (group.is(AwaitingSync) && generationId == group.generationId) {
                    if (errorCode != Errors.NONE.code) {
                      resetAndPropagateAssignmentError(group, errorCode)
                      maybePrepareRebalance(group)
                    } else {
                      setAndPropagateAssignment(group, assignment)
                      group.transitionTo(Stable)
                    }
                  }
                }
              }))
            }

          case Stable =>
            // if the group is stable, we just return the current assignment
            val memberMetadata = group.get(memberId)
            responseCallback(memberMetadata.assignment, Errors.NONE.code)
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))
        }
      }
    }

    // store the group metadata without holding the group lock to avoid the potential
    // for deadlock when the callback is invoked
    delayedGroupStore.foreach(groupManager.store)
  }

  def handleLeaveGroup(groupId: String, consumerId: String, responseCallback: Short => Unit) {
    if (!isActive.get) {
      responseCallback(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      responseCallback(Errors.GROUP_LOAD_IN_PROGRESS.code)
    } else {
      val group = groupManager.getGroup(groupId)
      if (group == null) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
        // joining without specified consumer id,
        responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
      } else {
        group synchronized {
          if (group.is(Dead)) {
            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
          } else if (!group.has(consumerId)) {
            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
          } else {
            val member = group.get(consumerId)
            removeHeartbeatForLeavingMember(group, member)
            onMemberFailure(group, member)
            responseCallback(Errors.NONE.code)
          }
        }
      }
    }
  }

  def handleHeartbeat(groupId: String,
                      memberId: String,
                      generationId: Int,
                      responseCallback: Short => Unit) {
    if (!isActive.get) {
      responseCallback(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      // the group is still loading, so respond just blindly
      responseCallback(Errors.NONE.code)
    } else {
      val group = groupManager.getGroup(groupId)
      if (group == null) {
        responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
      } else {
        group synchronized {
          if (group.is(Dead)) {
            // if the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; this is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // joining without the specified member id,
            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
          } else if (!group.is(Stable)) {
            responseCallback(Errors.REBALANCE_IN_PROGRESS.code)
          } else if (!group.has(memberId)) {
            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
          } else if (generationId != group.generationId) {
            responseCallback(Errors.ILLEGAL_GENERATION.code)
          } else {
            val member = group.get(memberId)
            completeAndScheduleNextHeartbeatExpiration(group, member)
            responseCallback(Errors.NONE.code)
          }
        }
      }
    }
  }

  def handleCommitOffsets(groupId: String,
                          memberId: String,
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Short] => Unit) {
    var delayedOffsetStore: Option[DelayedStore] = None

    if (!isActive.get) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code))
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.NOT_COORDINATOR_FOR_GROUP.code))
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.GROUP_LOAD_IN_PROGRESS.code))
    } else {
      val group = groupManager.getGroup(groupId)
      if (group == null) {
        if (generationId < 0)
          // the group is not relying on Kafka for partition management, so allow the commit
          delayedOffsetStore = Some(groupManager.prepareStoreOffsets(groupId, memberId, generationId, offsetMetadata,
            responseCallback))
        else
          // the group has failed over to this coordinator (which will be handled in KAFKA-2017),
          // or this is a request coming from an older generation. either way, reject the commit
          responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION.code))
      } else {
        group synchronized {
          if (group.is(Dead)) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
          } else if (group.is(AwaitingSync)) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.REBALANCE_IN_PROGRESS.code))
          } else if (!group.has(memberId)) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
          } else if (generationId != group.generationId) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION.code))
          } else {
            val member = group.get(memberId)
            completeAndScheduleNextHeartbeatExpiration(group, member)
            delayedOffsetStore = Some(groupManager.prepareStoreOffsets(groupId, memberId, generationId,
              offsetMetadata, responseCallback))
          }
        }
      }
    }

    // store the offsets without holding the group lock
    delayedOffsetStore.foreach(groupManager.store)
  }

  def handleFetchOffsets(groupId: String,
                         partitions: Seq[TopicPartition]): Map[TopicPartition, OffsetFetchResponse.PartitionData] = {
    if (!isActive.get) {
      partitions.map { case topicPartition =>
        (topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code))}.toMap
    } else if (!isCoordinatorForGroup(groupId)) {
      partitions.map { case topicPartition =>
        (topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.NOT_COORDINATOR_FOR_GROUP.code))}.toMap
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      partitions.map { case topicPartition =>
        (topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.GROUP_LOAD_IN_PROGRESS.code))}.toMap
    } else {
      // return offsets blindly regardless the current group state since the group may be using
      // Kafka commit storage without automatic group management
      groupManager.getOffsets(groupId, partitions)
    }
  }

  def handleListGroups(): (Errors, List[GroupOverview]) = {
    if (!isActive.get) {
      (Errors.GROUP_COORDINATOR_NOT_AVAILABLE, List[GroupOverview]())
    } else {
      val errorCode = if (groupManager.isLoading()) Errors.GROUP_LOAD_IN_PROGRESS else Errors.NONE
      (errorCode, groupManager.currentGroups.map(_.overview).toList)
    }
  }

  def handleDescribeGroup(groupId: String): (Errors, GroupSummary) = {
    if (!isActive.get) {
      (Errors.GROUP_COORDINATOR_NOT_AVAILABLE, GroupCoordinator.EmptyGroup)
    } else if (!isCoordinatorForGroup(groupId)) {
      (Errors.NOT_COORDINATOR_FOR_GROUP, GroupCoordinator.EmptyGroup)
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      (Errors.GROUP_LOAD_IN_PROGRESS, GroupCoordinator.EmptyGroup)
    } else {
      val group = groupManager.getGroup(groupId)
      if (group == null) {
        (Errors.NONE, GroupCoordinator.DeadGroup)
      } else {
        group synchronized {
          (Errors.NONE, group.summary)
        }
      }
    }
  }

  private def onGroupUnloaded(group: GroupMetadata) {
    group synchronized {
      info(s"Unloading group metadata for ${group.groupId} with generation ${group.generationId}")
      val previousState = group.currentState
      group.transitionTo(Dead)

      previousState match {
        case Dead =>
        case PreparingRebalance =>
          for (member <- group.allMemberMetadata) {
            if (member.awaitingJoinCallback != null) {
              member.awaitingJoinCallback(joinError(member.memberId, Errors.NOT_COORDINATOR_FOR_GROUP.code))
              member.awaitingJoinCallback = null
            }
          }
          joinPurgatory.checkAndComplete(GroupKey(group.groupId))

        case Stable | AwaitingSync =>
          for (member <- group.allMemberMetadata) {
            if (member.awaitingSyncCallback != null) {
              member.awaitingSyncCallback(Array.empty[Byte], Errors.NOT_COORDINATOR_FOR_GROUP.code)
              member.awaitingSyncCallback = null
            }
            heartbeatPurgatory.checkAndComplete(MemberKey(member.groupId, member.memberId))
          }
      }
    }
  }

  private def onGroupLoaded(group: GroupMetadata) {
    group synchronized {
      info(s"Loading group metadata for ${group.groupId} with generation ${group.generationId}")
      assert(group.is(Stable))
      group.allMemberMetadata.foreach(completeAndScheduleNextHeartbeatExpiration(group, _))
    }
  }

  def handleGroupImmigration(offsetTopicPartitionId: Int) {
    groupManager.loadGroupsForPartition(offsetTopicPartitionId, onGroupLoaded)
  }

  def handleGroupEmigration(offsetTopicPartitionId: Int) {
    groupManager.removeGroupsForPartition(offsetTopicPartitionId, onGroupUnloaded)
  }

  private def setAndPropagateAssignment(group: GroupMetadata, assignment: Map[String, Array[Byte]]) {
    assert(group.is(AwaitingSync))
    group.allMemberMetadata.foreach(member => member.assignment = assignment(member.memberId))
    propagateAssignment(group, Errors.NONE.code)
  }

  private def resetAndPropagateAssignmentError(group: GroupMetadata, errorCode: Short) {
    assert(group.is(AwaitingSync))
    group.allMemberMetadata.foreach(_.assignment = Array.empty[Byte])
    propagateAssignment(group, errorCode)
  }

  private def propagateAssignment(group: GroupMetadata, errorCode: Short) {
    for (member <- group.allMemberMetadata) {
      if (member.awaitingSyncCallback != null) {
        member.awaitingSyncCallback(member.assignment, errorCode)
        member.awaitingSyncCallback = null

        // reset the session timeout for members after propagating the member's assignment.
        // This is because if any member's session expired while we were still awaiting either
        // the leader sync group or the storage callback, its expiration will be ignored and no
        // future heartbeat expectations will not be scheduled.
        completeAndScheduleNextHeartbeatExpiration(group, member)
      }
    }
  }

  private def validGroupId(groupId: String): Boolean = {
    groupId != null && !groupId.isEmpty
  }

  private def joinError(memberId: String, errorCode: Short): JoinGroupResult = {
    JoinGroupResult(
      members=Map.empty,
      memberId=memberId,
      generationId=0,
      subProtocol=GroupCoordinator.NoProtocol,
      leaderId=GroupCoordinator.NoLeader,
      errorCode=errorCode)
  }

  /**
   * Complete existing DelayedHeartbeats for the given member and schedule the next one
   */
  private def completeAndScheduleNextHeartbeatExpiration(group: GroupMetadata, member: MemberMetadata) {
    // complete current heartbeat expectation
    member.latestHeartbeat = time.milliseconds()
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)

    // reschedule the next heartbeat expiration deadline
    val newHeartbeatDeadline = member.latestHeartbeat + member.sessionTimeoutMs
    val delayedHeartbeat = new DelayedHeartbeat(this, group, member, newHeartbeatDeadline, member.sessionTimeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
  }

  private def removeHeartbeatForLeavingMember(group: GroupMetadata, member: MemberMetadata) {
    member.isLeaving = true
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)
  }

  private def addMemberAndRebalance(sessionTimeoutMs: Int,
                                    clientId: String,
                                    clientHost: String,
                                    protocols: List[(String, Array[Byte])],
                                    group: GroupMetadata,
                                    callback: JoinCallback) = {
    // use the client-id with a random id suffix as the member-id
    val memberId = clientId + "-" + group.generateMemberIdSuffix
    val member = new MemberMetadata(memberId, group.groupId, clientId, clientHost, sessionTimeoutMs, protocols)
    member.awaitingJoinCallback = callback
    group.add(member.memberId, member)
    maybePrepareRebalance(group)
    member
  }

  private def updateMemberAndRebalance(group: GroupMetadata,
                                       member: MemberMetadata,
                                       protocols: List[(String, Array[Byte])],
                                       callback: JoinCallback) {
    member.supportedProtocols = protocols
    member.awaitingJoinCallback = callback
    maybePrepareRebalance(group)
  }

  private def maybePrepareRebalance(group: GroupMetadata) {
    group synchronized {
      if (group.canRebalance)
        prepareRebalance(group)
    }
  }

  private def prepareRebalance(group: GroupMetadata) {
    // if any members are awaiting sync, cancel their request and have them rejoin
    if (group.is(AwaitingSync))
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS.code)

    group.transitionTo(PreparingRebalance)
    info("Preparing to restabilize group %s with old generation %s".format(group.groupId, group.generationId))

    val rebalanceTimeout = group.rebalanceTimeout
    val delayedRebalance = new DelayedJoin(this, group, rebalanceTimeout)
    val groupKey = GroupKey(group.groupId)
    joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
  }

  private def onMemberFailure(group: GroupMetadata, member: MemberMetadata) {
    trace("Member %s in group %s has failed".format(member.memberId, group.groupId))
    group.remove(member.memberId)
    group.currentState match {
      case Dead =>
      case Stable | AwaitingSync => maybePrepareRebalance(group)
      case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  def tryCompleteJoin(group: GroupMetadata, forceComplete: () => Boolean) = {
    group synchronized {
      if (group.notYetRejoinedMembers.isEmpty)
        forceComplete()
      else false
    }
  }

  def onExpireJoin() {
    // TODO: add metrics for restabilize timeouts
  }

  def onCompleteJoin(group: GroupMetadata) {
    group synchronized {
      val failedMembers = group.notYetRejoinedMembers
      if (group.isEmpty || !failedMembers.isEmpty) {
        failedMembers.foreach { failedMember =>
          group.remove(failedMember.memberId)
          // TODO: cut the socket connection to the client
        }

        // TODO KAFKA-2720: only remove group in the background thread
        if (group.isEmpty) {
          group.transitionTo(Dead)
          groupManager.removeGroup(group)
          info("Group %s generation %s is dead and removed".format(group.groupId, group.generationId))
        }
      }
      if (!group.is(Dead)) {
        group.initNextGeneration()
        info("Stabilized group %s generation %s".format(group.groupId, group.generationId))

        // trigger the awaiting join group response callback for all the members after rebalancing
        for (member <- group.allMemberMetadata) {
          assert(member.awaitingJoinCallback != null)
          val joinResult = JoinGroupResult(
            members=if (member.memberId == group.leaderId) { group.currentMemberMetadata } else { Map.empty },
            memberId=member.memberId,
            generationId=group.generationId,
            subProtocol=group.protocol,
            leaderId=group.leaderId,
            errorCode=Errors.NONE.code)

          member.awaitingJoinCallback(joinResult)
          member.awaitingJoinCallback = null
          completeAndScheduleNextHeartbeatExpiration(group, member)
        }
      }
    }
  }

  def tryCompleteHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long, forceComplete: () => Boolean) = {
    group synchronized {
      if (shouldKeepMemberAlive(member, heartbeatDeadline) || member.isLeaving)
        forceComplete()
      else false
    }
  }

  def onExpireHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long) {
    group synchronized {
      if (!shouldKeepMemberAlive(member, heartbeatDeadline))
        onMemberFailure(group, member)
    }
  }

  def onCompleteHeartbeat() {
    // TODO: add metrics for complete heartbeats
  }

  def partitionFor(group: String): Int = groupManager.partitionFor(group)

  private def shouldKeepMemberAlive(member: MemberMetadata, heartbeatDeadline: Long) =
    member.awaitingJoinCallback != null ||
      member.awaitingSyncCallback != null ||
      member.latestHeartbeat + member.sessionTimeoutMs > heartbeatDeadline

  private def isCoordinatorForGroup(groupId: String) = groupManager.isGroupLocal(groupId)

  private def isCoordinatorLoadingInProgress(groupId: String) = groupManager.isGroupLoading(groupId)
}

object GroupCoordinator {

  val NoState = ""
  val NoProtocolType = ""
  val NoProtocol = ""
  val NoLeader = ""
  val NoMembers = List[MemberSummary]()
  val EmptyGroup = GroupSummary(NoState, NoProtocolType, NoProtocol, NoMembers)
  val DeadGroup = GroupSummary(Dead.toString, NoProtocolType, NoProtocol, NoMembers)

  def apply(config: KafkaConfig,
            zkUtils: ZkUtils,
            replicaManager: ReplicaManager,
            time: Time): GroupCoordinator = {
    val heartbeatPurgatory = DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", config.brokerId)
    val joinPurgatory = DelayedOperationPurgatory[DelayedJoin]("Rebalance", config.brokerId)
    apply(config, zkUtils, replicaManager, heartbeatPurgatory, joinPurgatory, time)
  }

  def apply(config: KafkaConfig,
            zkUtils: ZkUtils,
            replicaManager: ReplicaManager,
            heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
            joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
            time: Time): GroupCoordinator = {
    val offsetConfig = OffsetConfig(maxMetadataSize = config.offsetMetadataMaxSize,
      loadBufferSize = config.offsetsLoadBufferSize,
      offsetsRetentionMs = config.offsetsRetentionMinutes * 60 * 1000L,
      offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
      offsetsTopicNumPartitions = config.offsetsTopicPartitions,
      offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
      offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
      offsetCommitRequiredAcks = config.offsetCommitRequiredAcks)
    val groupConfig = GroupConfig(groupMinSessionTimeoutMs = config.groupMinSessionTimeoutMs,
      groupMaxSessionTimeoutMs = config.groupMaxSessionTimeoutMs)

    val groupManager = new GroupMetadataManager(config.brokerId, offsetConfig, replicaManager, zkUtils, time)
    new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, groupManager, heartbeatPurgatory, joinPurgatory, time)
  }

}
