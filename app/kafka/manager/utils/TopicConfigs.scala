/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.utils

import java.util.Properties

import kafka.manager.model._


trait TopicConfigs {
  def configNames : Set[String]
  def validate(props: Properties)
}

object TopicConfigs {
  
  val topicConfigsByVersion : Map[KafkaVersion, TopicConfigs] = Map(
    Kafka_0_8_1_1 -> zero81.LogConfig,
    Kafka_0_8_2_0 -> zero82.LogConfig,
    Kafka_0_8_2_1 -> zero82.LogConfig,
    Kafka_0_8_2_2 -> zero82.LogConfig,
    Kafka_0_9_0_0 -> zero90.LogConfig,
    Kafka_0_9_0_1 -> zero90.LogConfig,
    Kafka_0_10_0_0 -> zero90.LogConfig,
    Kafka_0_10_0_1 -> zero90.LogConfig,
    Kafka_0_10_1_0 -> zero90.LogConfig,
    Kafka_0_10_1_1 -> zero90.LogConfig,
    Kafka_0_10_2_0 -> zero90.LogConfig,
    Kafka_0_10_2_1 -> zero90.LogConfig,
    Kafka_0_11_0_0 -> zero90.LogConfig
    )

  def configNames(version: KafkaVersion) : Set[String] = {
    topicConfigsByVersion.get(version) match {
      case Some(tc) => tc.configNames
      case None => throw new IllegalArgumentException(s"Undefined topic configs for version : $version, cannot get config names")
    }
  }
  def validate(version: KafkaVersion, props: Properties) : Unit = {
    topicConfigsByVersion.get(version) match {
      case Some(tc) => tc.validate(props)
      case None => throw new IllegalArgumentException(s"Undefined topic configs for version : $version, cannot validate config")
    }
  }
}
