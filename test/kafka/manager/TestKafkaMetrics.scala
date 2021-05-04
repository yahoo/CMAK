/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import kafka.manager.jmx.KafkaMetrics
import kafka.manager.model._
import org.scalatest.funsuite.AnyFunSuite

/**
 * @author hiral
 */
class TestKafkaMetrics extends AnyFunSuite {
  test("generate broker metric name correctly for kafka 0.8.1.1") {
    val on = KafkaMetrics.getObjectName(Kafka_0_8_1_1,"MessagesInPerSec",None)
    assert(on.getCanonicalName === """"kafka.server":name="AllTopicsMessagesInPerSec",type="BrokerTopicMetrics"""")
  }
  test("generate broker metric name correctly for kafka 0.8.2") {
    val on = KafkaMetrics.getObjectName(Kafka_0_8_2_1,"MessagesInPerSec",None)
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics""")
  }
  test("generate broker metric name correctly for kafka 0.9.0") {
    val on = KafkaMetrics.getObjectName(Kafka_0_9_0_1,"MessagesInPerSec",None)
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics""")
  }
  test("generate broker metric name correctly for kafka 0.10.0") {
    val on = KafkaMetrics.getObjectName(Kafka_0_10_0_1,"MessagesInPerSec",None)
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics""")
  }
  test("generate broker metric name correctly for kafka 0.10.1") {
    val on = KafkaMetrics.getObjectName(Kafka_0_10_1_1,"MessagesInPerSec",None)
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics""")
  }
  test("generate broker metric name correctly for kafka 0.10.2") {
    val on = KafkaMetrics.getObjectName(Kafka_0_10_2_1,"MessagesInPerSec",None)
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics""")
  }
  test("generate broker metric name correctly for kafka 0.11.0") {
    val on = KafkaMetrics.getObjectName(Kafka_0_11_0_2,"MessagesInPerSec",None)
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics""")
  }
  test("generate broker metric name correctly for kafka 1.0.0") {
    val on = KafkaMetrics.getObjectName(Kafka_1_0_0,"MessagesInPerSec",None)
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics""")
  }
  test("generate broker metric name correctly for kafka 1.1.0") {
    val on = KafkaMetrics.getObjectName(Kafka_1_1_0,"MessagesInPerSec",None)
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics""")
  }
  test("generate broker metric name correctly for kafka 1.1.1") {
    val on = KafkaMetrics.getObjectName(Kafka_1_1_1,"MessagesInPerSec",None)
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
    test("generate topic metric name correctly for kafka 0.9.0") {
    val on = KafkaMetrics.getObjectName(Kafka_0_9_0_1,"MessagesInPerSec",Some("topic"))
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,topic=topic,type=BrokerTopicMetrics""")
  }
  test("generate topic metric name correctly for kafka 0.10.0") {
    val on = KafkaMetrics.getObjectName(Kafka_0_10_0_1,"MessagesInPerSec",Some("topic"))
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,topic=topic,type=BrokerTopicMetrics""")
  }
  test("generate topic metric name correctly for kafka 0.10.1") {
    val on = KafkaMetrics.getObjectName(Kafka_0_10_1_1,"MessagesInPerSec",Some("topic"))
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,topic=topic,type=BrokerTopicMetrics""")
  }
  test("generate topic metric name correctly for kafka 0.10.2") {
    val on = KafkaMetrics.getObjectName(Kafka_0_10_2_1,"MessagesInPerSec",Some("topic"))
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,topic=topic,type=BrokerTopicMetrics""")
  }
  test("generate topic metric name correctly for kafka 0.11.0") {
    val on = KafkaMetrics.getObjectName(Kafka_0_11_0_2,"MessagesInPerSec",Some("topic"))
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,topic=topic,type=BrokerTopicMetrics""")
  }
  test("generate topic metric name correctly for kafka 1.0.0") {
    val on = KafkaMetrics.getObjectName(Kafka_1_0_0,"MessagesInPerSec",Some("topic"))
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,topic=topic,type=BrokerTopicMetrics""")
  }
  test("generate topic metric name correctly for kafka 1.1.0") {
    val on = KafkaMetrics.getObjectName(Kafka_1_1_0,"MessagesInPerSec",Some("topic"))
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,topic=topic,type=BrokerTopicMetrics""")
  }
  test("generate topic metric name correctly for kafka 1.1.1") {
    val on = KafkaMetrics.getObjectName(Kafka_1_1_1,"MessagesInPerSec",Some("topic"))
    assert(on.getCanonicalName === """kafka.server:name=MessagesInPerSec,topic=topic,type=BrokerTopicMetrics""")
  }
}
