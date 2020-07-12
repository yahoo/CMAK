/**
  * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
  * See accompanying LICENSE file.
  */

package kafka.manager.utils

import java.util.Properties

import kafka.manager.model.{Kafka_1_0_0, _}


trait TopicConfigs {
  def configNames: Seq[String]

  def validate(props: Properties)

  def configNamesAndDoc: Seq[(String, String)]
}

object TopicConfigs {

  val topicConfigsByVersion: Map[KafkaVersion, TopicConfigs] = Map(
    Kafka_0_8_1_1 -> zero81.LogConfig,
    Kafka_0_8_2_0 -> zero82.LogConfig,
    Kafka_0_8_2_1 -> zero82.LogConfig,
    Kafka_0_8_2_2 -> zero82.LogConfig,
    Kafka_0_9_0_0 -> zero90.LogConfig,
    Kafka_0_9_0_1 -> zero90.LogConfig,
    Kafka_0_10_0_0 -> zero10.LogConfig,
    Kafka_0_10_0_1 -> zero10.LogConfig,
    Kafka_0_10_1_0 -> zero10.LogConfig,
    Kafka_0_10_1_1 -> zero10.LogConfig,
    Kafka_0_10_2_0 -> zero10.LogConfig,
    Kafka_0_10_2_1 -> zero10.LogConfig,
    Kafka_0_11_0_0 -> zero11.LogConfig,
    Kafka_0_11_0_2 -> zero11.LogConfig,
    Kafka_1_0_0 -> one10.LogConfig,
    Kafka_1_0_1 -> one10.LogConfig,
    Kafka_1_1_0 -> one10.LogConfig,
    Kafka_1_1_1 -> one10.LogConfig,
    Kafka_2_0_0 -> two00.LogConfig,
    Kafka_2_1_0 -> two00.LogConfig,
    Kafka_2_1_1 -> two00.LogConfig,
    Kafka_2_2_0 -> two00.LogConfig,
    Kafka_2_4_0 -> two00.LogConfig,
    Kafka_2_4_1 -> two00.LogConfig
  )

  def configNames(version: KafkaVersion): Seq[String] = {
    topicConfigsByVersion.get(version) match {
      case Some(tc) => tc.configNames
      case None => throw new IllegalArgumentException(s"Undefined topic configs for version : $version, cannot get config names")
    }
  }

  def validate(version: KafkaVersion, props: Properties): Unit = {
    topicConfigsByVersion.get(version) match {
      case Some(tc) => tc.validate(props)
      case None => throw new IllegalArgumentException(s"Undefined topic configs for version : $version, cannot validate config")
    }
  }

  def configNamesAndDoc(version: KafkaVersion): Seq[(String, String)] = {
    topicConfigsByVersion.get(version) match {
      case Some(tc) => tc.configNamesAndDoc
      case None => throw new IllegalArgumentException(s"Undefined topic configs for version : $version, cannot get config names and doc")
    }
  }
}
