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

package kafka.server

import java.net.SocketTimeoutException

import kafka.admin.AdminUtils
import kafka.cluster.BrokerEndPoint
import kafka.log.LogConfig
import kafka.message.ByteBufferMessageSet
import kafka.api.{KAFKA_0_10_0_IV0, KAFKA_0_9_0}
import kafka.common.{KafkaStorageException, TopicAndPartition}
import ReplicaFetcherThread._
import org.apache.kafka.clients.{ManualMetadataUpdater, NetworkClient, ClientRequest, ClientResponse}
import org.apache.kafka.common.network.{LoginType, Selectable, ChannelBuilders, NetworkReceive, Selector, Mode}
import org.apache.kafka.common.requests.{ListOffsetResponse, FetchResponse, RequestSend, AbstractRequest, ListOffsetRequest}
import org.apache.kafka.common.requests.{FetchRequest => JFetchRequest}
import org.apache.kafka.common.security.ssl.SslFactory
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{Errors, ApiKeys}
import org.apache.kafka.common.utils.Time

import scala.collection.{JavaConverters, Map, mutable}
import JavaConverters._

class ReplicaFetcherThread(name: String,
                           fetcherId: Int,
                           sourceBroker: BrokerEndPoint,
                           brokerConfig: KafkaConfig,
                           replicaMgr: ReplicaManager,
                           metrics: Metrics,
                           time: Time)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                sourceBroker = sourceBroker,
                                fetchBackOffMs = brokerConfig.replicaFetchBackoffMs,
                                isInterruptible = false) {

  type REQ = FetchRequest
  type PD = PartitionData

  private val fetchRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_0_IV0) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
    else 0
  private val socketTimeout: Int = brokerConfig.replicaSocketTimeoutMs
  private val replicaId = brokerConfig.brokerId
  private val maxWait = brokerConfig.replicaFetchWaitMaxMs
  private val minBytes = brokerConfig.replicaFetchMinBytes
  private val fetchSize = brokerConfig.replicaFetchMaxBytes

  private def clientId = name

  private val sourceNode = new Node(sourceBroker.id, sourceBroker.host, sourceBroker.port)

  // we need to include both the broker id and the fetcher id
  // as the metrics tag to avoid metric name conflicts with
  // more than one fetcher thread to the same broker
  private val networkClient = {
    val channelBuilder = ChannelBuilders.create(
      brokerConfig.interBrokerSecurityProtocol,
      Mode.CLIENT,
      LoginType.SERVER,
      brokerConfig.values,
      brokerConfig.saslMechanismInterBrokerProtocol,
      brokerConfig.saslInterBrokerHandshakeRequestEnable
    )
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      brokerConfig.connectionsMaxIdleMs,
      metrics,
      time,
      "replica-fetcher",
      Map("broker-id" -> sourceBroker.id.toString, "fetcher-id" -> fetcherId.toString).asJava,
      false,
      channelBuilder
    )
    new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      clientId,
      1,
      0,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      brokerConfig.replicaSocketReceiveBufferBytes,
      brokerConfig.requestTimeoutMs,
      time
    )
  }

  override def shutdown(): Unit = {
    super.shutdown()
    networkClient.close()
  }

  // process fetched data
  def processPartitionData(topicAndPartition: TopicAndPartition, fetchOffset: Long, partitionData: PartitionData) {
    try {
      val TopicAndPartition(topic, partitionId) = topicAndPartition
      val replica = replicaMgr.getReplica(topic, partitionId).get
      val messageSet = partitionData.toByteBufferMessageSet
      warnIfMessageOversized(messageSet, topicAndPartition)

      if (fetchOffset != replica.logEndOffset.messageOffset)
        throw new RuntimeException("Offset mismatch for partition %s: fetched offset = %d, log end offset = %d.".format(topicAndPartition, fetchOffset, replica.logEndOffset.messageOffset))
      if (logger.isTraceEnabled)
        trace("Follower %d has replica log end offset %d for partition %s. Received %d messages and leader hw %d"
          .format(replica.brokerId, replica.logEndOffset.messageOffset, topicAndPartition, messageSet.sizeInBytes, partitionData.highWatermark))
      replica.log.get.append(messageSet, assignOffsets = false)
      if (logger.isTraceEnabled)
        trace("Follower %d has replica log end offset %d after appending %d bytes of messages for partition %s"
          .format(replica.brokerId, replica.logEndOffset.messageOffset, messageSet.sizeInBytes, topicAndPartition))
      val followerHighWatermark = replica.logEndOffset.messageOffset.min(partitionData.highWatermark)
      // for the follower replica, we do not need to keep
      // its segment base offset the physical position,
      // these values will be computed upon making the leader
      replica.highWatermark = new LogOffsetMetadata(followerHighWatermark)
      if (logger.isTraceEnabled)
        trace("Follower %d set replica high watermark for partition [%s,%d] to %s"
          .format(replica.brokerId, topic, partitionId, followerHighWatermark))
    } catch {
      case e: KafkaStorageException =>
        fatal(s"Disk error while replicating data for $topicAndPartition", e)
        Runtime.getRuntime.halt(1)
    }
  }

  def warnIfMessageOversized(messageSet: ByteBufferMessageSet, topicAndPartition: TopicAndPartition): Unit = {
    if (messageSet.sizeInBytes > 0 && messageSet.validBytes <= 0)
      error(s"Replication is failing due to a message that is greater than replica.fetch.max.bytes for partition $topicAndPartition. " +
        "This generally occurs when the max.message.bytes has been overridden to exceed this value and a suitably large " +
        "message has also been sent. To fix this problem increase replica.fetch.max.bytes in your broker config to be " +
        "equal or larger than your settings for max.message.bytes, both at a broker and topic level.")
  }

  /**
   * Handle a partition whose offset is out of range and return a new fetch offset.
   */
  def handleOffsetOutOfRange(topicAndPartition: TopicAndPartition): Long = {
    val replica = replicaMgr.getReplica(topicAndPartition.topic, topicAndPartition.partition).get

    /**
     * Unclean leader election: A follower goes down, in the meanwhile the leader keeps appending messages. The follower comes back up
     * and before it has completely caught up with the leader's logs, all replicas in the ISR go down. The follower is now uncleanly
     * elected as the new leader, and it starts appending messages from the client. The old leader comes back up, becomes a follower
     * and it may discover that the current leader's end offset is behind its own end offset.
     *
     * In such a case, truncate the current follower's log to the current leader's end offset and continue fetching.
     *
     * There is a potential for a mismatch between the logs of the two replicas here. We don't fix this mismatch as of now.
     */
    val leaderEndOffset: Long = earliestOrLatestOffset(topicAndPartition, ListOffsetRequest.LATEST_TIMESTAMP,
      brokerConfig.brokerId)

    if (leaderEndOffset < replica.logEndOffset.messageOffset) {
      // Prior to truncating the follower's log, ensure that doing so is not disallowed by the configuration for unclean leader election.
      // This situation could only happen if the unclean election configuration for a topic changes while a replica is down. Otherwise,
      // we should never encounter this situation since a non-ISR leader cannot be elected if disallowed by the broker configuration.
      if (!LogConfig.fromProps(brokerConfig.originals, AdminUtils.fetchEntityConfig(replicaMgr.zkUtils,
        ConfigType.Topic, topicAndPartition.topic)).uncleanLeaderElectionEnable) {
        // Log a fatal error and shutdown the broker to ensure that data loss does not unexpectedly occur.
        fatal("Halting because log truncation is not allowed for topic %s,".format(topicAndPartition.topic) +
          " Current leader %d's latest offset %d is less than replica %d's latest offset %d"
          .format(sourceBroker.id, leaderEndOffset, brokerConfig.brokerId, replica.logEndOffset.messageOffset))
        Runtime.getRuntime.halt(1)
      }

      warn("Replica %d for partition %s reset its fetch offset from %d to current leader %d's latest offset %d"
        .format(brokerConfig.brokerId, topicAndPartition, replica.logEndOffset.messageOffset, sourceBroker.id, leaderEndOffset))
      replicaMgr.logManager.truncateTo(Map(topicAndPartition -> leaderEndOffset))
      leaderEndOffset
    } else {
      /**
       * If the leader's log end offset is greater than the follower's log end offset, there are two possibilities:
       * 1. The follower could have been down for a long time and when it starts up, its end offset could be smaller than the leader's
       * start offset because the leader has deleted old logs (log.logEndOffset < leaderStartOffset).
       * 2. When unclean leader election occurs, it is possible that the old leader's high watermark is greater than
       * the new leader's log end offset. So when the old leader truncates its offset to its high watermark and starts
       * to fetch from the new leader, an OffsetOutOfRangeException will be thrown. After that some more messages are
       * produced to the new leader. While the old leader is trying to handle the OffsetOutOfRangeException and query
       * the log end offset of the new leader, the new leader's log end offset becomes higher than the follower's log end offset.
       *
       * In the first case, the follower's current log end offset is smaller than the leader's log start offset. So the
       * follower should truncate all its logs, roll out a new segment and start to fetch from the current leader's log
       * start offset.
       * In the second case, the follower should just keep the current log segments and retry the fetch. In the second
       * case, there will be some inconsistency of data between old and new leader. We are not solving it here.
       * If users want to have strong consistency guarantees, appropriate configurations needs to be set for both
       * brokers and producers.
       *
       * Putting the two cases together, the follower should fetch from the higher one of its replica log end offset
       * and the current leader's log start offset.
       *
       */
      val leaderStartOffset: Long = earliestOrLatestOffset(topicAndPartition, ListOffsetRequest.EARLIEST_TIMESTAMP,
        brokerConfig.brokerId)
      warn("Replica %d for partition %s reset its fetch offset from %d to current leader %d's start offset %d"
        .format(brokerConfig.brokerId, topicAndPartition, replica.logEndOffset.messageOffset, sourceBroker.id, leaderStartOffset))
      val offsetToFetch = Math.max(leaderStartOffset, replica.logEndOffset.messageOffset)
      // Only truncate log when current leader's log start offset is greater than follower's log end offset.
      if (leaderStartOffset > replica.logEndOffset.messageOffset)
        replicaMgr.logManager.truncateFullyAndStartAt(topicAndPartition, leaderStartOffset)
      offsetToFetch
    }
  }

  // any logic for partitions whose leader has changed
  def handlePartitionsWithErrors(partitions: Iterable[TopicAndPartition]) {
    delayPartitions(partitions, brokerConfig.replicaFetchBackoffMs.toLong)
  }

  protected def fetch(fetchRequest: FetchRequest): Map[TopicAndPartition, PartitionData] = {
    val clientResponse = sendRequest(ApiKeys.FETCH, Some(fetchRequestVersion), fetchRequest.underlying)
    new FetchResponse(clientResponse.responseBody).responseData.asScala.map { case (key, value) =>
      TopicAndPartition(key.topic, key.partition) -> new PartitionData(value)
    }
  }

  private def sendRequest(apiKey: ApiKeys, apiVersion: Option[Short], request: AbstractRequest): ClientResponse = {
    import kafka.utils.NetworkClientBlockingOps._
    val header = apiVersion.fold(networkClient.nextRequestHeader(apiKey))(networkClient.nextRequestHeader(apiKey, _))
    try {
      if (!networkClient.blockingReady(sourceNode, socketTimeout)(time))
        throw new SocketTimeoutException(s"Failed to connect within $socketTimeout ms")
      else {
        val send = new RequestSend(sourceBroker.id.toString, header, request.toStruct)
        val clientRequest = new ClientRequest(time.milliseconds(), true, send, null)
        networkClient.blockingSendAndReceive(clientRequest)(time)
      }
    }
    catch {
      case e: Throwable =>
        networkClient.close(sourceBroker.id.toString)
        throw e
    }

  }

  private def earliestOrLatestOffset(topicAndPartition: TopicAndPartition, earliestOrLatest: Long, consumerId: Int): Long = {
    val topicPartition = new TopicPartition(topicAndPartition.topic, topicAndPartition.partition)
    val partitions = Map(
      topicPartition -> new ListOffsetRequest.PartitionData(earliestOrLatest, 1)
    )
    val request = new ListOffsetRequest(consumerId, partitions.asJava)
    val clientResponse = sendRequest(ApiKeys.LIST_OFFSETS, None, request)
    val response = new ListOffsetResponse(clientResponse.responseBody)
    val partitionData = response.responseData.get(topicPartition)
    Errors.forCode(partitionData.errorCode) match {
      case Errors.NONE => partitionData.offsets.asScala.head
      case errorCode => throw errorCode.exception
    }
  }

  protected def buildFetchRequest(partitionMap: Map[TopicAndPartition, PartitionFetchState]): FetchRequest = {
    val requestMap = mutable.Map.empty[TopicPartition, JFetchRequest.PartitionData]

    partitionMap.foreach { case ((TopicAndPartition(topic, partition), partitionFetchState)) =>
      if (partitionFetchState.isActive)
        requestMap(new TopicPartition(topic, partition)) = new JFetchRequest.PartitionData(partitionFetchState.offset, fetchSize)
    }

    new FetchRequest(new JFetchRequest(replicaId, maxWait, minBytes, requestMap.asJava))
  }

}

object ReplicaFetcherThread {

  private[server] class FetchRequest(val underlying: JFetchRequest) extends AbstractFetcherThread.FetchRequest {
    def isEmpty: Boolean = underlying.fetchData.isEmpty
    def offset(topicAndPartition: TopicAndPartition): Long =
      underlying.fetchData.asScala(new TopicPartition(topicAndPartition.topic, topicAndPartition.partition)).offset
  }

  private[server] class PartitionData(val underlying: FetchResponse.PartitionData) extends AbstractFetcherThread.PartitionData {

    def errorCode: Short = underlying.errorCode

    def toByteBufferMessageSet: ByteBufferMessageSet = new ByteBufferMessageSet(underlying.recordSet)

    def highWatermark: Long = underlying.highWatermark

    def exception: Option[Throwable] = Errors.forCode(errorCode) match {
      case Errors.NONE => None
      case e => Some(e.exception)
    }

  }

}
