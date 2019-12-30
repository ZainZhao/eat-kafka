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

package kafka.javaapi

import kafka.common.TopicAndPartition

class OffsetResponse(private val underlying: kafka.api.OffsetResponse) {

  def hasError = underlying.hasError


  def errorCode(topic: String, partition: Int) =
    underlying.partitionErrorAndOffsets(TopicAndPartition(topic, partition)).error


  def offsets(topic: String, partition: Int) =
    underlying.partitionErrorAndOffsets(TopicAndPartition(topic, partition)).offsets.toArray


  override def equals(other: Any) = canEqual(other) && {
    val otherOffsetResponse = other.asInstanceOf[kafka.javaapi.OffsetResponse]
    this.underlying.equals(otherOffsetResponse.underlying)
  }


  def canEqual(other: Any) = other.isInstanceOf[kafka.javaapi.OffsetResponse]


  override def hashCode = underlying.hashCode


  override def toString = underlying.toString

}
