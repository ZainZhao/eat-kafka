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

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.test.MockProcessorNode;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.MockTimestampExtractor;
import org.junit.Test;
import org.junit.Before;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StreamTaskTest {

    private final Serializer<Integer> intSerializer = new IntegerSerializer();
    private final Deserializer<Integer> intDeserializer = new IntegerDeserializer();
    private final Serializer<byte[]> bytesSerializer = new ByteArraySerializer();

    private final TopicPartition partition1 = new TopicPartition("topic1", 1);
    private final TopicPartition partition2 = new TopicPartition("topic2", 1);
    private final Set<TopicPartition> partitions = Utils.mkSet(partition1, partition2);

    private final MockSourceNode<Integer, Integer> source1 = new MockSourceNode<>(intDeserializer, intDeserializer);
    private final MockSourceNode<Integer, Integer> source2 = new MockSourceNode<>(intDeserializer, intDeserializer);
    private final MockProcessorNode<Integer, Integer>  processor = new MockProcessorNode<>(10L);

    private final ProcessorTopology topology = new ProcessorTopology(
            Arrays.asList((ProcessorNode) source1, (ProcessorNode) source2, (ProcessorNode) processor),
            new HashMap<String, SourceNode>() {
                {
                    put("topic1", source1);
                    put("topic2", source2);
                }
            },
            Collections.<StateStoreSupplier>emptyList()
    );

    private StreamsConfig createConfig(final File baseDir) throws Exception {
        return new StreamsConfig(new Properties() {
            {
                setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "stream-task-test");
                setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171");
                setProperty(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3");
                setProperty(StreamsConfig.STATE_DIR_CONFIG, baseDir.getCanonicalPath());
                setProperty(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName());
            }
        });
    }

    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final MockProducer<byte[], byte[]> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer);
    private final MockConsumer<byte[], byte[]> restoreStateConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

    private final byte[] recordValue = intSerializer.serialize(null, 10);
    private final byte[] recordKey = intSerializer.serialize(null, 1);


    @Before
    public void setup() {
        consumer.assign(Arrays.asList(partition1, partition2));
        source1.addChild(processor);
        source2.addChild(processor);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProcessOrder() throws Exception {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            StreamsConfig config = createConfig(baseDir);
            StreamTask task = new StreamTask(new TaskId(0, 0), "applicationId", partitions, topology, consumer, producer, restoreStateConsumer, config, null);

            task.addRecords(partition1, records(
                    new ConsumerRecord<>(partition1.topic(), partition1.partition(), 10, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                    new ConsumerRecord<>(partition1.topic(), partition1.partition(), 20, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                    new ConsumerRecord<>(partition1.topic(), partition1.partition(), 30, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
            ));

            task.addRecords(partition2, records(
                    new ConsumerRecord<>(partition2.topic(), partition2.partition(), 25, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                    new ConsumerRecord<>(partition2.topic(), partition2.partition(), 35, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                    new ConsumerRecord<>(partition2.topic(), partition2.partition(), 45, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
            ));

            assertEquals(5, task.process());
            assertEquals(1, source1.numReceived);
            assertEquals(0, source2.numReceived);

            assertEquals(4, task.process());
            assertEquals(2, source1.numReceived);
            assertEquals(0, source2.numReceived);

            assertEquals(3, task.process());
            assertEquals(2, source1.numReceived);
            assertEquals(1, source2.numReceived);

            assertEquals(2, task.process());
            assertEquals(3, source1.numReceived);
            assertEquals(1, source2.numReceived);

            assertEquals(1, task.process());
            assertEquals(3, source1.numReceived);
            assertEquals(2, source2.numReceived);

            assertEquals(0, task.process());
            assertEquals(3, source1.numReceived);
            assertEquals(3, source2.numReceived);

            task.close();

        } finally {
            Utils.delete(baseDir);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPauseResume() throws Exception {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            StreamsConfig config = createConfig(baseDir);
            StreamTask task = new StreamTask(new TaskId(1, 1), "applicationId", partitions, topology, consumer, producer, restoreStateConsumer, config, null);

            task.addRecords(partition1, records(
                    new ConsumerRecord<>(partition1.topic(), partition1.partition(), 10, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                    new ConsumerRecord<>(partition1.topic(), partition1.partition(), 20, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
            ));

            task.addRecords(partition2, records(
                    new ConsumerRecord<>(partition2.topic(), partition2.partition(), 35, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                    new ConsumerRecord<>(partition2.topic(), partition2.partition(), 45, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                    new ConsumerRecord<>(partition2.topic(), partition2.partition(), 55, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                    new ConsumerRecord<>(partition2.topic(), partition2.partition(), 65, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
            ));

            assertEquals(5, task.process());
            assertEquals(1, source1.numReceived);
            assertEquals(0, source2.numReceived);

            assertEquals(1, consumer.paused().size());
            assertTrue(consumer.paused().contains(partition2));

            task.addRecords(partition1, records(
                    new ConsumerRecord<>(partition1.topic(), partition1.partition(), 30, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                    new ConsumerRecord<>(partition1.topic(), partition1.partition(), 40, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                    new ConsumerRecord<>(partition1.topic(), partition1.partition(), 50, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
            ));

            assertEquals(2, consumer.paused().size());
            assertTrue(consumer.paused().contains(partition1));
            assertTrue(consumer.paused().contains(partition2));

            assertEquals(7, task.process());
            assertEquals(2, source1.numReceived);
            assertEquals(0, source2.numReceived);

            assertEquals(1, consumer.paused().size());
            assertTrue(consumer.paused().contains(partition2));

            assertEquals(6, task.process());
            assertEquals(3, source1.numReceived);
            assertEquals(0, source2.numReceived);

            assertEquals(1, consumer.paused().size());
            assertTrue(consumer.paused().contains(partition2));

            assertEquals(5, task.process());
            assertEquals(3, source1.numReceived);
            assertEquals(1, source2.numReceived);

            assertEquals(0, consumer.paused().size());

            task.close();

        } finally {
            Utils.delete(baseDir);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMaybePunctuate() throws Exception {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            StreamsConfig config = createConfig(baseDir);
            StreamTask task = new StreamTask(new TaskId(0, 0), "applicationId", partitions, topology, consumer, producer, restoreStateConsumer, config, null);

            task.addRecords(partition1, records(
                    new ConsumerRecord<>(partition1.topic(), partition1.partition(), 20, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                    new ConsumerRecord<>(partition1.topic(), partition1.partition(), 30, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                    new ConsumerRecord<>(partition1.topic(), partition1.partition(), 40, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
            ));

            task.addRecords(partition2, records(
                    new ConsumerRecord<>(partition2.topic(), partition2.partition(), 25, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                    new ConsumerRecord<>(partition2.topic(), partition2.partition(), 35, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                    new ConsumerRecord<>(partition2.topic(), partition2.partition(), 45, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
            ));

            assertTrue(task.maybePunctuate());

            assertEquals(5, task.process());
            assertEquals(1, source1.numReceived);
            assertEquals(0, source2.numReceived);

            assertFalse(task.maybePunctuate());

            assertEquals(4, task.process());
            assertEquals(1, source1.numReceived);
            assertEquals(1, source2.numReceived);

            assertTrue(task.maybePunctuate());

            assertEquals(3, task.process());
            assertEquals(2, source1.numReceived);
            assertEquals(1, source2.numReceived);

            assertFalse(task.maybePunctuate());

            assertEquals(2, task.process());
            assertEquals(2, source1.numReceived);
            assertEquals(2, source2.numReceived);

            assertTrue(task.maybePunctuate());

            assertEquals(1, task.process());
            assertEquals(3, source1.numReceived);
            assertEquals(2, source2.numReceived);

            assertFalse(task.maybePunctuate());

            assertEquals(0, task.process());
            assertEquals(3, source1.numReceived);
            assertEquals(3, source2.numReceived);

            assertFalse(task.maybePunctuate());

            processor.supplier.checkAndClearPunctuateResult(20L, 30L, 40L);

            task.close();

        } finally {
            Utils.delete(baseDir);
        }
    }

    private Iterable<ConsumerRecord<byte[], byte[]>> records(ConsumerRecord<byte[], byte[]>... recs) {
        return Arrays.asList(recs);
    }
}
