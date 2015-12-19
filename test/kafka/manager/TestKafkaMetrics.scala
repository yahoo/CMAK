/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import kafka.manager.jmx.KafkaMetrics
import kafka.manager.model.{Kafka_0_8_2_1, Kafka_0_8_1_1}
import org.scalatest.FunSuite

/**
 * @author hiral
 */
class TestKafkaMetrics extends FunSuite {
  test("generate broker metric name correctly for kafka 0.8.1.1") {
    val on = KafkaMetrics.getObjectName(Kafka_0_8_1_1,"MessagesInPerSec",None)
    assert(on.getCanonicalName === """"kafka.server":name="AllTopicsMessagesInPerSec",type="BrokerTopicMetrics"""")
  }
  test("generate broker metric name correctly for kafka 0.8.2") {
    val on = KafkaMetrics.getObjectName(Kafka_0_8_2_1,"MessagesInPerSec",None)
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics""")
  }
  test("generate topic metric name correctly for kafka 0.8.1.1") {
    val on = KafkaMetrics.getObjectName(Kafka_0_8_1_1,"MessagesInPerSec",Some("topic"))
    assert(on.getCanonicalName === """"kafka.server":name="topic-MessagesInPerSec",type="BrokerTopicMetrics"""")
  }
  test("generate topic metric name correctly for kafka 0.8.2") {
    val on = KafkaMetrics.getObjectName(Kafka_0_8_2_1,"MessagesInPerSec",Some("topic"))
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,topic=topic,type=BrokerTopicMetrics""")
  }
}
