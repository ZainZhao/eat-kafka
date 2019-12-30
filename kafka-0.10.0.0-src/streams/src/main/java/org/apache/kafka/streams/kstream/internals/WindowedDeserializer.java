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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.Windowed;

import java.nio.ByteBuffer;
import java.util.Map;

public class WindowedDeserializer<T> implements Deserializer<Windowed<T>> {

    private static final int TIMESTAMP_SIZE = 8;

    private Deserializer<T> inner;

    public WindowedDeserializer(Deserializer<T> inner) {
        this.inner = inner;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public Windowed<T> deserialize(String topic, byte[] data) {

        byte[] bytes = new byte[data.length - TIMESTAMP_SIZE];

        System.arraycopy(data, 0, bytes, 0, bytes.length);

        long start = ByteBuffer.wrap(data).getLong(data.length - TIMESTAMP_SIZE);

        // always read as unlimited window
        return new Windowed<T>(inner.deserialize(topic, bytes), new UnlimitedWindow(start));
    }


    @Override
    public void close() {
        inner.close();
    }
}
