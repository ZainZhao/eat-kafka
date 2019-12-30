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

package kafka.log

import java.util.Properties

import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigException
import org.junit.{Assert, Test}
import org.junit.Assert._
import org.scalatest.Assertions._

class LogConfigTest {

  @Test
  def testKafkaConfigToProps() {
    val millisInHour = 60L * 60L * 1000L
    val kafkaProps = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
    kafkaProps.put(KafkaConfig.LogRollTimeHoursProp, "2")
    kafkaProps.put(KafkaConfig.LogRollTimeJitterHoursProp, "2")
    kafkaProps.put(KafkaConfig.LogRetentionTimeHoursProp, "2")

    val kafkaConfig = KafkaConfig.fromProps(kafkaProps)
    val logProps = KafkaServer.copyKafkaConfigToLog(kafkaConfig)
    assertEquals(2 * millisInHour, logProps.get(LogConfig.SegmentMsProp))
    assertEquals(2 * millisInHour, logProps.get(LogConfig.SegmentJitterMsProp))
    assertEquals(2 * millisInHour, logProps.get(LogConfig.RetentionMsProp))
  }

  @Test
  def testFromPropsEmpty() {
    val p = new Properties()
    val config = LogConfig(p)
    Assert.assertEquals(LogConfig(), config)
  }

  @Test
  def testFromPropsInvalid() {
    LogConfig.configNames.foreach(name => name match {
      case LogConfig.UncleanLeaderElectionEnableProp => assertPropertyInvalid(name, "not a boolean")
      case LogConfig.RetentionBytesProp => assertPropertyInvalid(name, "not_a_number")
      case LogConfig.RetentionMsProp => assertPropertyInvalid(name, "not_a_number" )
      case LogConfig.CleanupPolicyProp => assertPropertyInvalid(name, "true", "foobar");
      case LogConfig.MinCleanableDirtyRatioProp => assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2")
      case LogConfig.MinInSyncReplicasProp => assertPropertyInvalid(name, "not_a_number", "0", "-1")
      case LogConfig.MessageFormatVersionProp => assertPropertyInvalid(name, "")
      case positiveIntProperty => assertPropertyInvalid(name, "not_a_number", "-1")
    })
  }

  private def assertPropertyInvalid(name: String, values: AnyRef*) {
    values.foreach((value) => {
      val props = new Properties
      props.setProperty(name, value.toString)
      intercept[Exception] {
        LogConfig(props)
      }
    })
  }

  private def randFrom[T](choices: T*): T = {
    import scala.util.Random
    choices(Random.nextInt(choices.size))
  }
}
