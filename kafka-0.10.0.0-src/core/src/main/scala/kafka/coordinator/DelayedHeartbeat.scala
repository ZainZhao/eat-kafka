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

import kafka.server.DelayedOperation

/**
 * Delayed heartbeat operations that are added to the purgatory for session timeout checking.
 * Heartbeats are paused during rebalance.
 */
private[coordinator] class DelayedHeartbeat(coordinator: GroupCoordinator,
                                            group: GroupMetadata,
                                            member: MemberMetadata,
                                            heartbeatDeadline: Long,
                                            sessionTimeout: Long)
  extends DelayedOperation(sessionTimeout) {
  override def tryComplete(): Boolean = coordinator.tryCompleteHeartbeat(group, member, heartbeatDeadline, forceComplete)
  override def onExpiration() = coordinator.onExpireHeartbeat(group, member, heartbeatDeadline)
  override def onComplete() = coordinator.onCompleteHeartbeat()
}
