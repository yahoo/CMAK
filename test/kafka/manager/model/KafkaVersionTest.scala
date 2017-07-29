/**
 * Copyright 2017 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager.model

import org.scalatest.FunSuite

/**
  * @author fuji-151a
  */
class KafkaVersionTest extends FunSuite {

  test("check supportedVersion") {
    val expected: Map[String, KafkaVersion] = Map(
      "0.8.1.1" -> Kafka_0_8_1_1,
      "0.8.2-beta" -> Kafka_0_8_2_0,
      "0.8.2.0" -> Kafka_0_8_2_0,
      "0.8.2.1" -> Kafka_0_8_2_1,
      "0.8.2.2" -> Kafka_0_8_2_2,
      "0.9.0.0" -> Kafka_0_9_0_0,
      "0.9.0.1" -> Kafka_0_9_0_1,
      "0.10.0.0" -> Kafka_0_10_0_0,
      "0.10.0.1" -> Kafka_0_10_0_1,
      "0.10.1.0" -> Kafka_0_10_1_0,
      "0.10.1.1" -> Kafka_0_10_1_1,
      "0.10.2.0" -> Kafka_0_10_2_0,
      "0.10.2.1" -> Kafka_0_10_2_1
    )
    assertResult(expected)(KafkaVersion.supportedVersions)
  }

  test("Sort formSelectList") {
    val expected: IndexedSeq[(String,String)] = Vector(
      ("0.8.1.1","0.8.1.1"),
      ("0.8.2.0","0.8.2.0"),
      ("0.8.2.1","0.8.2.1"),
      ("0.8.2.2","0.8.2.2"),
      ("0.9.0.0","0.9.0.0"),
      ("0.9.0.1","0.9.0.1"),
      ("0.10.0.0","0.10.0.0"),
      ("0.10.0.1","0.10.0.1"),
      ("0.10.1.0","0.10.1.0"),
      ("0.10.1.1","0.10.1.1"),
      ("0.10.2.0","0.10.2.0"),
      ("0.10.2.1","0.10.2.1")
    )
    assertResult(expected)(KafkaVersion.formSelectList)
  }

}