/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package models.form

/**
 * @author hiral
 */

sealed trait TopicOperation

case class CreateTopic(topic: String, partitions: Int, replication: Int) extends TopicOperation
case class DeleteTopic(topic: String) extends TopicOperation
case class AlterTopic(topic: String, partitions: Int)
