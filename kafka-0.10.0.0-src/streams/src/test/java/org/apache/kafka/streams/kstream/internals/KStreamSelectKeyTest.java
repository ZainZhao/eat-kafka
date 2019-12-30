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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class KStreamSelectKeyTest {

    private String topicName = "topic_key_select";

    final private Serde<Integer> integerSerde = Serdes.Integer();
    final private Serde<String> stringSerde = Serdes.String();

    private KStreamTestDriver driver;

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test
    public void testSelectKey() {
        KStreamBuilder builder = new KStreamBuilder();

        final Map<Integer, String> keyMap = new HashMap<>();
        keyMap.put(1, "ONE");
        keyMap.put(2, "TWO");
        keyMap.put(3, "THREE");


        KeyValueMapper<String, Integer, String> selector = new KeyValueMapper<String, Integer, String>() {
            @Override
            public String apply(String key, Integer value) {
                return keyMap.get(value);
            }
        };

        final String[] expected = new String[]{"ONE:1", "TWO:2", "THREE:3"};
        final int[] expectedValues = new int[]{1, 2, 3};

        KStream<String, Integer>  stream = builder.stream(stringSerde, integerSerde, topicName);

        MockProcessorSupplier<String, Integer> processor = new MockProcessorSupplier<>();

        stream.selectKey(selector).process(processor);

        driver = new KStreamTestDriver(builder);

        for (int expectedValue : expectedValues) {
            driver.process(topicName, null, expectedValue);
        }

        assertEquals(3, processor.processed.size());

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }

    }

}