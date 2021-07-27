/**
  * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
  * See accompanying LICENSE file.
  */

package kafka.manager.utils

import java.util.Properties

import kafka.manager.model.{KafkaVersion, Kafka_0_10_1_1, Kafka_0_8_1_1}


trait BrokerConfigs {
  def configNames: Seq[String]

  def validate(props: Properties)

  def configNamesAndDoc: Seq[(String, String)]
}
object BrokerConfigs{
  val brokerConfigsByVersion: Map[KafkaVersion, BrokerConfigs] = Map(
    Kafka_0_10_1_1 -> zero10.BrokerConfig,
  )

  def configNames(version: KafkaVersion): Seq[String] = {
    brokerConfigsByVersion.get(version) match {
      case Some(tc) => tc.configNames
      case None => throw new IllegalArgumentException(s"Undefined topic configs for version : $version, cannot get config names")
    }
  }

  def validate(version: KafkaVersion, props: Properties): Unit = {
    brokerConfigsByVersion.get(version) match {
      case Some(tc) => tc.validate(props)
      case None => throw new IllegalArgumentException(s"Undefined topic configs for version : $version, cannot validate config")
    }
  }

  def configNamesAndDoc(version: KafkaVersion): Seq[(String, String)] = {
    brokerConfigsByVersion.get(version) match {
      case Some(tc) => tc.configNamesAndDoc
      case None => throw new IllegalArgumentException(s"Undefined topic configs for version : $version, cannot get config names and doc")
    }
  }
}


