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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;


public class KStreamImplTest {

    final private Serde<String> stringSerde = Serdes.String();
    final private Serde<Integer> intSerde = Serdes.Integer();

    @Test
    public void testNumProcesses() {
        final KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> source1 = builder.stream(stringSerde, stringSerde, "topic-1", "topic-2");

        KStream<String, String> source2 = builder.stream(stringSerde, stringSerde, "topic-3", "topic-4");

        KStream<String, String> stream1 =
            source1.filter(new Predicate<String, String>() {
                @Override
                public boolean test(String key, String value) {
                    return true;
                }
            }).filterNot(new Predicate<String, String>() {
                @Override
                public boolean test(String key, String value) {
                    return false;
                }
            });

        KStream<String, Integer> stream2 = stream1.mapValues(new ValueMapper<String, Integer>() {
            @Override
            public Integer apply(String value) {
                return new Integer(value);
            }
        });

        KStream<String, Integer> stream3 = source2.flatMapValues(new ValueMapper<String, Iterable<Integer>>() {
            @Override
            public Iterable<Integer> apply(String value) {
                return Collections.singletonList(new Integer(value));
            }
        });

        KStream<String, Integer>[] streams2 = stream2.branch(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return (value % 2) == 0;
                    }
                },
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return true;
                    }
                }
        );

        KStream<String, Integer>[] streams3 = stream3.branch(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return (value % 2) == 0;
                    }
                },
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return true;
                    }
                }
        );

        KStream<String, Integer> stream4 = streams2[0].join(streams3[0], new ValueJoiner<Integer, Integer, Integer>() {
            @Override
            public Integer apply(Integer value1, Integer value2) {
                return value1 + value2;
            }
        }, JoinWindows.of("join-0"), stringSerde, intSerde, intSerde);

        KStream<String, Integer> stream5 = streams2[1].join(streams3[1], new ValueJoiner<Integer, Integer, Integer>() {
            @Override
            public Integer apply(Integer value1, Integer value2) {
                return value1 + value2;
            }
        }, JoinWindows.of("join-1"), stringSerde, intSerde, intSerde);

        stream4.to("topic-5");

        streams2[1].through("topic-6").process(new MockProcessorSupplier<String, Integer>());

        assertEquals(2 + // sources
            2 + // stream1
            1 + // stream2
            1 + // stream3
            1 + 2 + // streams2
            1 + 2 + // streams3
            5 * 2 + // stream2-stream3 joins
            1 + // to
            2 + // through
            1, // process
            builder.build("X", null).processors().size());
    }

    @Test
    public void testToWithNullValueSerdeDoesntNPE() {
        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, String> inputStream = builder.stream(stringSerde, stringSerde, "input");
        inputStream.to(stringSerde, null, "output");
    }
}
