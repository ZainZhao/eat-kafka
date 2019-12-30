# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.tests.test import Test
from ducktape.mark import parametrize

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.performance import ProducerPerformanceService
from kafkatest.services.console_consumer import ConsoleConsumer


class QuotaTest(Test):
    """
    These tests verify that quota provides expected functionality -- they run
    producer, broker, and consumer with different clientId and quota configuration and
    check that the observed throughput is close to the value we expect.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(QuotaTest, self).__init__(test_context=test_context)

        self.topic = 'test_topic'
        self.logger.info('use topic ' + self.topic)

        # quota related parameters
        self.quota_config = {'quota_producer_default': 2500000,
                             'quota_consumer_default': 2000000,
                             'quota_producer_bytes_per_second_overrides': 'overridden_id=3750000',
                             'quota_consumer_bytes_per_second_overrides': 'overridden_id=3000000'}
        self.maximum_client_deviation_percentage = 100.0
        self.maximum_broker_deviation_percentage = 5.0
        self.num_records = 100000
        self.record_size = 3000

        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context, num_nodes=1, zk=self.zk,
                                  security_protocol='PLAINTEXT',
                                  interbroker_security_protocol='PLAINTEXT',
                                  topics={self.topic: {'partitions': 6, 'replication-factor': 1, 'configs': {'min.insync.replicas': 1}}},
                                  quota_config=self.quota_config,
                                  jmx_object_names=['kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec',
                                                    'kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec'],
                                  jmx_attributes=['OneMinuteRate'])
        self.num_producers = 1
        self.num_consumers = 2

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    def min_cluster_size(self):
        """Override this since we're adding services outside of the constructor"""
        return super(QuotaTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    @parametrize(producer_id='default_id', producer_num=1, consumer_id='default_id', consumer_num=1)
    @parametrize(producer_id='overridden_id', producer_num=1, consumer_id='overridden_id', consumer_num=1)
    @parametrize(producer_id='overridden_id', producer_num=1, consumer_id='overridden_id', consumer_num=2)
    def test_quota(self, producer_id='default_id', producer_num=1, consumer_id='default_id', consumer_num=1):
        # Produce all messages
        producer = ProducerPerformanceService(
            self.test_context, producer_num, self.kafka,
            topic=self.topic, num_records=self.num_records, record_size=self.record_size, throughput=-1, client_id=producer_id,
            jmx_object_names=['kafka.producer:type=producer-metrics,client-id=%s' % producer_id], jmx_attributes=['outgoing-byte-rate'])

        producer.run()

        # Consume all messages
        consumer = ConsoleConsumer(self.test_context, consumer_num, self.kafka, self.topic,
            new_consumer=False,
            consumer_timeout_ms=60000, client_id=consumer_id,
            jmx_object_names=['kafka.consumer:type=ConsumerTopicMetrics,name=BytesPerSec,clientId=%s' % consumer_id],
            jmx_attributes=['OneMinuteRate'])
        consumer.run()

        for idx, messages in consumer.messages_consumed.iteritems():
            assert len(messages) > 0, "consumer %d didn't consume any message before timeout" % idx

        success, msg = self.validate(self.kafka, producer, consumer)
        assert success, msg

    def validate(self, broker, producer, consumer):
        """
        For each client_id we validate that:
        1) number of consumed messages equals number of produced messages
        2) maximum_producer_throughput <= producer_quota * (1 + maximum_client_deviation_percentage/100)
        3) maximum_broker_byte_in_rate <= producer_quota * (1 + maximum_broker_deviation_percentage/100)
        4) maximum_consumer_throughput <= consumer_quota * (1 + maximum_client_deviation_percentage/100)
        5) maximum_broker_byte_out_rate <= consumer_quota * (1 + maximum_broker_deviation_percentage/100)
        """
        success = True
        msg = ''

        self.kafka.read_jmx_output_all_nodes()

        # validate that number of consumed messages equals number of produced messages
        produced_num = sum([value['records'] for value in producer.results])
        consumed_num = sum([len(value) for value in consumer.messages_consumed.values()])
        self.logger.info('producer produced %d messages' % produced_num)
        self.logger.info('consumer consumed %d messages' % consumed_num)
        if produced_num != consumed_num:
            success = False
            msg += "number of produced messages %d doesn't equal number of consumed messages %d" % (produced_num, consumed_num)

        # validate that maximum_producer_throughput <= producer_quota * (1 + maximum_client_deviation_percentage/100)
        producer_attribute_name = 'kafka.producer:type=producer-metrics,client-id=%s:outgoing-byte-rate' % producer.client_id
        producer_maximum_bps = producer.maximum_jmx_value[producer_attribute_name]
        producer_quota_bps = self.get_producer_quota(producer.client_id)
        self.logger.info('producer has maximum throughput %.2f bps with producer quota %.2f bps' % (producer_maximum_bps, producer_quota_bps))
        if producer_maximum_bps > producer_quota_bps*(self.maximum_client_deviation_percentage/100+1):
            success = False
            msg += 'maximum producer throughput %.2f bps exceeded producer quota %.2f bps by more than %.1f%%' % \
                   (producer_maximum_bps, producer_quota_bps, self.maximum_client_deviation_percentage)

        # validate that maximum_broker_byte_in_rate <= producer_quota * (1 + maximum_broker_deviation_percentage/100)
        broker_byte_in_attribute_name = 'kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec:OneMinuteRate'
        broker_maximum_byte_in_bps = broker.maximum_jmx_value[broker_byte_in_attribute_name]
        self.logger.info('broker has maximum byte-in rate %.2f bps with producer quota %.2f bps' %
                         (broker_maximum_byte_in_bps, producer_quota_bps))
        if broker_maximum_byte_in_bps > producer_quota_bps*(self.maximum_broker_deviation_percentage/100+1):
            success = False
            msg += 'maximum broker byte-in rate %.2f bps exceeded producer quota %.2f bps by more than %.1f%%' % \
                   (broker_maximum_byte_in_bps, producer_quota_bps, self.maximum_broker_deviation_percentage)

        # validate that maximum_consumer_throughput <= consumer_quota * (1 + maximum_client_deviation_percentage/100)
        consumer_attribute_name = 'kafka.consumer:type=ConsumerTopicMetrics,name=BytesPerSec,clientId=%s:OneMinuteRate' % consumer.client_id
        consumer_maximum_bps = consumer.maximum_jmx_value[consumer_attribute_name]
        consumer_quota_bps = self.get_consumer_quota(consumer.client_id)
        self.logger.info('consumer has maximum throughput %.2f bps with consumer quota %.2f bps' % (consumer_maximum_bps, consumer_quota_bps))
        if consumer_maximum_bps > consumer_quota_bps*(self.maximum_client_deviation_percentage/100+1):
            success = False
            msg += 'maximum consumer throughput %.2f bps exceeded consumer quota %.2f bps by more than %.1f%%' % \
                   (consumer_maximum_bps, consumer_quota_bps, self.maximum_client_deviation_percentage)

        # validate that maximum_broker_byte_out_rate <= consumer_quota * (1 + maximum_broker_deviation_percentage/100)
        broker_byte_out_attribute_name = 'kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec:OneMinuteRate'
        broker_maximum_byte_out_bps = broker.maximum_jmx_value[broker_byte_out_attribute_name]
        self.logger.info('broker has maximum byte-out rate %.2f bps with consumer quota %.2f bps' %
                         (broker_maximum_byte_out_bps, consumer_quota_bps))
        if broker_maximum_byte_out_bps > consumer_quota_bps*(self.maximum_broker_deviation_percentage/100+1):
            success = False
            msg += 'maximum broker byte-out rate %.2f bps exceeded consumer quota %.2f bps by more than %.1f%%' % \
                   (broker_maximum_byte_out_bps, consumer_quota_bps, self.maximum_broker_deviation_percentage)

        return success, msg

    def get_producer_quota(self, client_id):
        overridden_quotas = {value.split('=')[0]:value.split('=')[1] for value in self.quota_config['quota_producer_bytes_per_second_overrides'].split(',')}
        if client_id in overridden_quotas:
            return float(overridden_quotas[client_id])
        return self.quota_config['quota_producer_default']

    def get_consumer_quota(self, client_id):
        overridden_quotas = {value.split('=')[0]:value.split('=')[1] for value in self.quota_config['quota_consumer_bytes_per_second_overrides'].split(',')}
        if client_id in overridden_quotas:
            return float(overridden_quotas[client_id])
        return self.quota_config['quota_consumer_default']
