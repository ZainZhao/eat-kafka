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

package kafka.api

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import kafka.utils.TestUtils
import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import kafka.server.KafkaConfig
import kafka.integration.KafkaServerTestHarness
import org.junit.{After, Before}
import scala.collection.mutable.Buffer
import kafka.coordinator.GroupCoordinator
import org.apache.kafka.common.internals.TopicConstants

/**
 * A helper class for writing integration tests that involve producers, consumers, and servers
 */
trait IntegrationTestHarness extends KafkaServerTestHarness {

  val producerCount: Int
  val consumerCount: Int
  val serverCount: Int
  lazy val producerConfig = new Properties
  lazy val consumerConfig = new Properties
  lazy val serverConfig = new Properties

  val consumers = Buffer[KafkaConsumer[Array[Byte], Array[Byte]]]()
  val producers = Buffer[KafkaProducer[Array[Byte], Array[Byte]]]()

  override def generateConfigs() = {
    val cfgs = TestUtils.createBrokerConfigs(serverCount, zkConnect, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = saslProperties)
    cfgs.foreach(_.putAll(serverConfig))
    cfgs.map(KafkaConfig.fromProps)
  }

  @Before
  override def setUp() {
    val producerSecurityProps = TestUtils.producerSecurityConfigs(securityProtocol, trustStoreFile, saslProperties)
    val consumerSecurityProps = TestUtils.consumerSecurityConfigs(securityProtocol, trustStoreFile, saslProperties)
    super.setUp()
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArraySerializer])
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArraySerializer])
    producerConfig.putAll(producerSecurityProps)
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer])
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer])
    consumerConfig.putAll(consumerSecurityProps)
    for (i <- 0 until producerCount)
      producers += TestUtils.createNewProducer(brokerList,
                                               securityProtocol = this.securityProtocol,
                                               trustStoreFile = this.trustStoreFile,
                                               saslProperties = this.saslProperties,
                                               props = Some(producerConfig))
    for (i <- 0 until consumerCount) {
      consumers += TestUtils.createNewConsumer(brokerList,
                                               securityProtocol = this.securityProtocol,
                                               trustStoreFile = this.trustStoreFile,
                                               saslProperties = this.saslProperties,
                                               props = Some(consumerConfig))
    }

    // create the consumer offset topic
    TestUtils.createTopic(zkUtils, TopicConstants.GROUP_METADATA_TOPIC_NAME,
      serverConfig.getProperty(KafkaConfig.OffsetsTopicPartitionsProp).toInt,
      serverConfig.getProperty(KafkaConfig.OffsetsTopicReplicationFactorProp).toInt,
      servers,
      servers(0).consumerCoordinator.offsetsTopicConfigs)
  }

  @After
  override def tearDown() {
    producers.foreach(_.close())
    consumers.foreach(_.close())
    super.tearDown()
  }

}
