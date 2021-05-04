/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package models.form

import enumeratum.EnumEntry.{Snakecase, Uppercase}
import enumeratum.{Enum, EnumEntry}
import kafka.manager.model.ActorModel.BrokerIdentity

/**
 * @author hiral
 */
sealed abstract class ReassignPartitionOperation (override val entryName: String) extends EnumEntry with Snakecase with Uppercase

object ReassignPartitionOperation extends Enum[ReassignPartitionOperation] {
  val values = findValues

  case object RunAssignment extends ReassignPartitionOperation("run")
  case object ForceRunAssignment extends ReassignPartitionOperation("force")
  case object UnknownRPO extends ReassignPartitionOperation("unknown")

}

case class BrokerSelect(id: Int, host: String, selected: Boolean)
object BrokerSelect {
  implicit def from(bi: BrokerIdentity) : BrokerSelect = {
    BrokerSelect(bi.id,bi.host,true)
  }
}

case class TopicSelect(name: String, selected: Boolean)
object TopicSelect {
  implicit def from(topicName: String) : TopicSelect = {
    TopicSelect(topicName,true)
  }
}

case class ReadVersion(topic: String, version: Int)

case class GenerateAssignment(brokers: Seq[BrokerSelect], replicationFactor: Option[Int] = None)
case class GenerateMultipleAssignments(topics: Seq[TopicSelect], brokers: Seq[BrokerSelect])
case class RunMultipleAssignments(topics: Seq[TopicSelect])
