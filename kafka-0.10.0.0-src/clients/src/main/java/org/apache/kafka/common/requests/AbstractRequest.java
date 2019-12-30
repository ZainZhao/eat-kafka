/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public abstract class AbstractRequest extends AbstractRequestResponse {

    public AbstractRequest(Struct struct) {
        super(struct);
    }

    /**
     * Get an error response for a request for a given api version
     */
    public abstract AbstractRequestResponse getErrorResponse(int versionId, Throwable e);

    /**
     * Factory method for getting a request object based on ApiKey ID and a buffer
     */
    public static AbstractRequest getRequest(int requestId, int versionId, ByteBuffer buffer) {
        ApiKeys apiKey = ApiKeys.forId(requestId);
        switch (apiKey) {
            case PRODUCE:
                return ProduceRequest.parse(buffer, versionId);
            case FETCH:
                return FetchRequest.parse(buffer, versionId);
            case LIST_OFFSETS:
                return ListOffsetRequest.parse(buffer, versionId);
            case METADATA:
                return MetadataRequest.parse(buffer, versionId);
            case OFFSET_COMMIT:
                return OffsetCommitRequest.parse(buffer, versionId);
            case OFFSET_FETCH:
                return OffsetFetchRequest.parse(buffer, versionId);
            case GROUP_COORDINATOR:
                return GroupCoordinatorRequest.parse(buffer, versionId);
            case JOIN_GROUP:
                return JoinGroupRequest.parse(buffer, versionId);
            case HEARTBEAT:
                return HeartbeatRequest.parse(buffer, versionId);
            case LEAVE_GROUP:
                return LeaveGroupRequest.parse(buffer, versionId);
            case SYNC_GROUP:
                return SyncGroupRequest.parse(buffer, versionId);
            case STOP_REPLICA:
                return StopReplicaRequest.parse(buffer, versionId);
            case CONTROLLED_SHUTDOWN_KEY:
                return ControlledShutdownRequest.parse(buffer, versionId);
            case UPDATE_METADATA_KEY:
                return UpdateMetadataRequest.parse(buffer, versionId);
            case LEADER_AND_ISR:
                return LeaderAndIsrRequest.parse(buffer, versionId);
            case DESCRIBE_GROUPS:
                return DescribeGroupsRequest.parse(buffer, versionId);
            case LIST_GROUPS:
                return ListGroupsRequest.parse(buffer, versionId);
            case SASL_HANDSHAKE:
                return SaslHandshakeRequest.parse(buffer, versionId);
            case API_VERSIONS:
                return ApiVersionsRequest.parse(buffer, versionId);
            default:
                throw new AssertionError(String.format("ApiKey %s is not currently handled in `getRequest`, the " +
                        "code should be updated to do so.", apiKey));
        }
    }
}