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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SyncGroupRequest extends AbstractRequest {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.SYNC_GROUP.id);
    public static final String GROUP_ID_KEY_NAME = "group_id";
    public static final String GENERATION_ID_KEY_NAME = "generation_id";
    public static final String MEMBER_ID_KEY_NAME = "member_id";
    public static final String MEMBER_ASSIGNMENT_KEY_NAME = "member_assignment";
    public static final String GROUP_ASSIGNMENT_KEY_NAME = "group_assignment";

    private final String groupId;
    private final int generationId;
    private final String memberId;
    private final Map<String, ByteBuffer> groupAssignment;

    public SyncGroupRequest(String groupId,
                            int generationId,
                            String memberId,
                            Map<String, ByteBuffer> groupAssignment) {
        super(new Struct(CURRENT_SCHEMA));
        struct.set(GROUP_ID_KEY_NAME, groupId);
        struct.set(GENERATION_ID_KEY_NAME, generationId);
        struct.set(MEMBER_ID_KEY_NAME, memberId);

        List<Struct> memberArray = new ArrayList<>();
        for (Map.Entry<String, ByteBuffer> entries: groupAssignment.entrySet()) {
            Struct memberData = struct.instance(GROUP_ASSIGNMENT_KEY_NAME);
            memberData.set(MEMBER_ID_KEY_NAME, entries.getKey());
            memberData.set(MEMBER_ASSIGNMENT_KEY_NAME, entries.getValue());
            memberArray.add(memberData);
        }
        struct.set(GROUP_ASSIGNMENT_KEY_NAME, memberArray.toArray());

        this.groupId = groupId;
        this.generationId = generationId;
        this.memberId = memberId;
        this.groupAssignment = groupAssignment;
    }

    public SyncGroupRequest(Struct struct) {
        super(struct);
        this.groupId = struct.getString(GROUP_ID_KEY_NAME);
        this.generationId = struct.getInt(GENERATION_ID_KEY_NAME);
        this.memberId = struct.getString(MEMBER_ID_KEY_NAME);

        groupAssignment = new HashMap<>();

        for (Object memberDataObj : struct.getArray(GROUP_ASSIGNMENT_KEY_NAME)) {
            Struct memberData = (Struct) memberDataObj;
            String memberId = memberData.getString(MEMBER_ID_KEY_NAME);
            ByteBuffer memberMetadata = memberData.getBytes(MEMBER_ASSIGNMENT_KEY_NAME);
            groupAssignment.put(memberId, memberMetadata);
        }
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        switch (versionId) {
            case 0:
                return new SyncGroupResponse(
                        Errors.forException(e).code(),
                        ByteBuffer.wrap(new byte[]{}));
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.JOIN_GROUP.id)));
        }
    }

    public String groupId() {
        return groupId;
    }

    public int generationId() {
        return generationId;
    }

    public Map<String, ByteBuffer> groupAssignment() {
        return groupAssignment;
    }

    public String memberId() {
        return memberId;
    }

    public static SyncGroupRequest parse(ByteBuffer buffer, int versionId) {
        return new SyncGroupRequest(ProtoUtils.parseRequest(ApiKeys.SYNC_GROUP.id, versionId, buffer));
    }

}
