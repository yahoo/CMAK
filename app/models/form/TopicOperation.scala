/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package models.form

/**
 * @author hiral
 */

sealed trait TopicOperation

case class TConfig(name: String, value: Option[String])

case class CreateTopic(topic: String, partitions: Int, replication: Int, configs: List[TConfig]) extends TopicOperation
case class DeleteTopic(topic: String) extends TopicOperation
case class AddTopicPartitions(topic: String, brokers: Seq[BrokerSelect], partitions: Int, readVersion: Int) extends TopicOperation
case class AddMultipleTopicsPartitions(topics: Seq[TopicSelect],brokers: Seq[BrokerSelect], partitions: Int, readVersions: Seq[ReadVersion]) extends TopicOperation
case class UpdateTopicConfig(topic: String, configs: List[TConfig], readVersion: Int) extends TopicOperation
case class UnknownTO(op: String) extends TopicOperation

