/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.storage;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Implementation of OffsetBackingStore that doesn't actually persist any data. To ensure this
 * behaves similarly to a real backing store, operations are executed asynchronously on a
 * background thread.
 */
public class MemoryOffsetBackingStore implements OffsetBackingStore {
    private static final Logger log = LoggerFactory.getLogger(MemoryOffsetBackingStore.class);

    protected Map<ByteBuffer, ByteBuffer> data = new HashMap<>();
    protected ExecutorService executor = Executors.newSingleThreadExecutor();

    public MemoryOffsetBackingStore() {

    }

    @Override
    public void configure(WorkerConfig config) {
    }

    @Override
    public synchronized void start() {
    }

    @Override
    public synchronized void stop() {
        // Nothing to do since this doesn't maintain any outstanding connections/data
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(
            final Collection<ByteBuffer> keys,
            final Callback<Map<ByteBuffer, ByteBuffer>> callback) {
        return executor.submit(new Callable<Map<ByteBuffer, ByteBuffer>>() {
            @Override
            public Map<ByteBuffer, ByteBuffer> call() throws Exception {
                Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
                synchronized (MemoryOffsetBackingStore.this) {
                    for (ByteBuffer key : keys) {
                        result.put(key, data.get(key));
                    }
                }
                if (callback != null)
                    callback.onCompletion(null, result);
                return result;
            }
        });

    }

    @Override
    public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values,
                            final Callback<Void> callback) {
        return executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                synchronized (MemoryOffsetBackingStore.this) {
                    for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
                        data.put(entry.getKey(), entry.getValue());
                    }
                    save();
                }
                if (callback != null)
                    callback.onCompletion(null, null);
                return null;
            }
        });
    }

    // Hook to allow subclasses to persist data
    protected void save() {

    }
}
