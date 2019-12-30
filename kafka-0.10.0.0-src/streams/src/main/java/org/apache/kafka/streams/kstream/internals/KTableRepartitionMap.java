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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * KTable repartition map functions are not exposed to public APIs, but only used for keyed aggregations.
 *
 * Given the input, it can output at most two records (one mapped from old value and one mapped from new value).
 */
public class KTableRepartitionMap<K, V, K1, V1> implements KTableProcessorSupplier<K, V, KeyValue<K1, V1>> {

    private final KTableImpl<K, ?, V> parent;
    private final KeyValueMapper<K, V, KeyValue<K1, V1>> mapper;

    public KTableRepartitionMap(KTableImpl<K, ?, V> parent, KeyValueMapper<K, V, KeyValue<K1, V1>> mapper) {
        this.parent = parent;
        this.mapper = mapper;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new KTableMapProcessor();
    }

    @Override
    public KTableValueGetterSupplier<K, KeyValue<K1, V1>> view() {
        final KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

        return new KTableValueGetterSupplier<K, KeyValue<K1, V1>>() {

            public KTableValueGetter<K, KeyValue<K1, V1>> get() {
                return new KTableMapValueGetter(parentValueGetterSupplier.get());
            }

        };
    }

    /**
     * @throws IllegalStateException since this method should never be called
     */
    @Override
    public void enableSendingOldValues() {
        // this should never be called
        throw new IllegalStateException("KTableRepartitionMap should always require sending old values.");
    }

    private KeyValue<K1, V1> computeValue(K key, V value) {
        KeyValue<K1, V1> newValue = null;

        if (key != null || value != null)
            newValue = mapper.apply(key, value);

        return newValue;
    }

    private class KTableMapProcessor extends AbstractProcessor<K, Change<V>> {

        /**
         * @throws StreamsException if key is null
         */
        @Override
        public void process(K key, Change<V> change) {
            KeyValue<K1, V1> newPair = computeValue(key, change.newValue);

            // the selected repartition key should never be null
            if (newPair.key == null)
                throw new StreamsException("Record key for KTable repartition operator should not be null.");

            context().forward(newPair.key, new Change<>(newPair.value, null));

            if (change.oldValue != null) {
                KeyValue<K1, V1> oldPair = computeValue(key, change.oldValue);
                context().forward(oldPair.key, new Change<>(null, oldPair.value));
            }
        }
    }

    private class KTableMapValueGetter implements KTableValueGetter<K, KeyValue<K1, V1>> {

        private final KTableValueGetter<K, V> parentGetter;

        public KTableMapValueGetter(KTableValueGetter<K, V> parentGetter) {
            this.parentGetter = parentGetter;
        }

        @Override
        public void init(ProcessorContext context) {
            parentGetter.init(context);
        }

        @Override
        public KeyValue<K1, V1> get(K key) {
            return computeValue(key, parentGetter.get(key));
        }

    }

}
