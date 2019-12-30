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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

public class KStreamWindowAggregateTest {

    final private Serde<String> strSerde = Serdes.String();

    private KStreamTestDriver driver = null;
    private File stateDir = null;

    @After
    public void tearDown() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Before
    public void setUp() throws IOException {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }

    @Test
    public void testAggBasic() throws Exception {
        final File baseDir = Files.createTempDirectory("test").toFile();

        try {
            final KStreamBuilder builder = new KStreamBuilder();
            String topic1 = "topic1";

            KStream<String, String> stream1 = builder.stream(strSerde, strSerde, topic1);
            KTable<Windowed<String>, String> table2 =
                stream1.aggregateByKey(MockInitializer.STRING_INIT, MockAggregator.STRING_ADDER,
                    TimeWindows.of("topic1-Canonized", 10).advanceBy(5),
                    strSerde,
                    strSerde);

            MockProcessorSupplier<Windowed<String>, String> proc2 = new MockProcessorSupplier<>();
            table2.toStream().process(proc2);

            KStreamTestDriver driver = new KStreamTestDriver(builder, baseDir);

            driver.setTime(0L);
            driver.process(topic1, "A", "1");
            driver.setTime(1L);
            driver.process(topic1, "B", "2");
            driver.setTime(2L);
            driver.process(topic1, "C", "3");
            driver.setTime(3L);
            driver.process(topic1, "D", "4");
            driver.setTime(4L);
            driver.process(topic1, "A", "1");

            driver.setTime(5L);
            driver.process(topic1, "A", "1");
            driver.setTime(6L);
            driver.process(topic1, "B", "2");
            driver.setTime(7L);
            driver.process(topic1, "D", "4");
            driver.setTime(8L);
            driver.process(topic1, "B", "2");
            driver.setTime(9L);
            driver.process(topic1, "C", "3");

            driver.setTime(10L);
            driver.process(topic1, "A", "1");
            driver.setTime(11L);
            driver.process(topic1, "B", "2");
            driver.setTime(12L);
            driver.process(topic1, "D", "4");
            driver.setTime(13L);
            driver.process(topic1, "B", "2");
            driver.setTime(14L);
            driver.process(topic1, "C", "3");

            assertEquals(Utils.mkList(
                    "[A@0]:0+1",
                    "[B@0]:0+2",
                    "[C@0]:0+3",
                    "[D@0]:0+4",
                    "[A@0]:0+1+1",

                    "[A@0]:0+1+1+1", "[A@5]:0+1",
                    "[B@0]:0+2+2", "[B@5]:0+2",
                    "[D@0]:0+4+4", "[D@5]:0+4",
                    "[B@0]:0+2+2+2", "[B@5]:0+2+2",
                    "[C@0]:0+3+3", "[C@5]:0+3",

                    "[A@5]:0+1+1", "[A@10]:0+1",
                    "[B@5]:0+2+2+2", "[B@10]:0+2",
                    "[D@5]:0+4+4", "[D@10]:0+4",
                    "[B@5]:0+2+2+2+2", "[B@10]:0+2+2",
                    "[C@5]:0+3+3", "[C@10]:0+3"), proc2.processed);

        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void testJoin() throws Exception {
        final File baseDir = Files.createTempDirectory("test").toFile();

        try {
            final KStreamBuilder builder = new KStreamBuilder();
            String topic1 = "topic1";
            String topic2 = "topic2";

            KStream<String, String> stream1 = builder.stream(strSerde, strSerde, topic1);
            KTable<Windowed<String>, String> table1 =
                stream1.aggregateByKey(MockInitializer.STRING_INIT, MockAggregator.STRING_ADDER,
                    TimeWindows.of("topic1-Canonized", 10).advanceBy(5),
                    strSerde,
                    strSerde);

            MockProcessorSupplier<Windowed<String>, String> proc1 = new MockProcessorSupplier<>();
            table1.toStream().process(proc1);

            KStream<String, String> stream2 = builder.stream(strSerde, strSerde, topic2);
            KTable<Windowed<String>, String> table2 =
                stream2.aggregateByKey(MockInitializer.STRING_INIT, MockAggregator.STRING_ADDER,
                    TimeWindows.of("topic2-Canonized", 10).advanceBy(5),
                    strSerde,
                    strSerde);

            MockProcessorSupplier<Windowed<String>, String> proc2 = new MockProcessorSupplier<>();
            table2.toStream().process(proc2);


            MockProcessorSupplier<Windowed<String>, String> proc3 = new MockProcessorSupplier<>();
            table1.join(table2, new ValueJoiner<String, String, String>() {
                @Override
                public String apply(String p1, String p2) {
                    return p1 + "%" + p2;
                }
            }).toStream().process(proc3);

            KStreamTestDriver driver = new KStreamTestDriver(builder, baseDir);

            driver.setTime(0L);
            driver.process(topic1, "A", "1");
            driver.setTime(1L);
            driver.process(topic1, "B", "2");
            driver.setTime(2L);
            driver.process(topic1, "C", "3");
            driver.setTime(3L);
            driver.process(topic1, "D", "4");
            driver.setTime(4L);
            driver.process(topic1, "A", "1");

            proc1.checkAndClearProcessResult(
                    "[A@0]:0+1",
                    "[B@0]:0+2",
                    "[C@0]:0+3",
                    "[D@0]:0+4",
                    "[A@0]:0+1+1"
            );
            proc2.checkAndClearProcessResult();
            proc3.checkAndClearProcessResult(
                    "[A@0]:null",
                    "[B@0]:null",
                    "[C@0]:null",
                    "[D@0]:null",
                    "[A@0]:null"
            );

            driver.setTime(5L);
            driver.process(topic1, "A", "1");
            driver.setTime(6L);
            driver.process(topic1, "B", "2");
            driver.setTime(7L);
            driver.process(topic1, "D", "4");
            driver.setTime(8L);
            driver.process(topic1, "B", "2");
            driver.setTime(9L);
            driver.process(topic1, "C", "3");

            proc1.checkAndClearProcessResult(
                    "[A@0]:0+1+1+1", "[A@5]:0+1",
                    "[B@0]:0+2+2", "[B@5]:0+2",
                    "[D@0]:0+4+4", "[D@5]:0+4",
                    "[B@0]:0+2+2+2", "[B@5]:0+2+2",
                    "[C@0]:0+3+3", "[C@5]:0+3"
            );
            proc2.checkAndClearProcessResult();
            proc3.checkAndClearProcessResult(
                    "[A@0]:null", "[A@5]:null",
                    "[B@0]:null", "[B@5]:null",
                    "[D@0]:null", "[D@5]:null",
                    "[B@0]:null", "[B@5]:null",
                    "[C@0]:null", "[C@5]:null"
            );

            driver.setTime(0L);
            driver.process(topic2, "A", "a");
            driver.setTime(1L);
            driver.process(topic2, "B", "b");
            driver.setTime(2L);
            driver.process(topic2, "C", "c");
            driver.setTime(3L);
            driver.process(topic2, "D", "d");
            driver.setTime(4L);
            driver.process(topic2, "A", "a");

            proc1.checkAndClearProcessResult();
            proc2.checkAndClearProcessResult(
                    "[A@0]:0+a",
                    "[B@0]:0+b",
                    "[C@0]:0+c",
                    "[D@0]:0+d",
                    "[A@0]:0+a+a"
            );
            proc3.checkAndClearProcessResult(
                    "[A@0]:0+1+1+1%0+a",
                    "[B@0]:0+2+2+2%0+b",
                    "[C@0]:0+3+3%0+c",
                    "[D@0]:0+4+4%0+d",
                    "[A@0]:0+1+1+1%0+a+a");

            driver.setTime(5L);
            driver.process(topic2, "A", "a");
            driver.setTime(6L);
            driver.process(topic2, "B", "b");
            driver.setTime(7L);
            driver.process(topic2, "D", "d");
            driver.setTime(8L);
            driver.process(topic2, "B", "b");
            driver.setTime(9L);
            driver.process(topic2, "C", "c");

            proc1.checkAndClearProcessResult();
            proc2.checkAndClearProcessResult(
                    "[A@0]:0+a+a+a", "[A@5]:0+a",
                    "[B@0]:0+b+b", "[B@5]:0+b",
                    "[D@0]:0+d+d", "[D@5]:0+d",
                    "[B@0]:0+b+b+b", "[B@5]:0+b+b",
                    "[C@0]:0+c+c", "[C@5]:0+c"
            );
            proc3.checkAndClearProcessResult(
                    "[A@0]:0+1+1+1%0+a+a+a", "[A@5]:0+1%0+a",
                    "[B@0]:0+2+2+2%0+b+b", "[B@5]:0+2+2%0+b",
                    "[D@0]:0+4+4%0+d+d", "[D@5]:0+4%0+d",
                    "[B@0]:0+2+2+2%0+b+b+b", "[B@5]:0+2+2%0+b+b",
                    "[C@0]:0+3+3%0+c+c", "[C@5]:0+3%0+c"
            );
        } finally {
            Utils.delete(baseDir);
        }
    }
}
