/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package models.form

import kafka.manager.ActorModel.BrokerIdentity

/**
 * @author hiral
 */
sealed trait ReassignPartitionOperation

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

case class GenerateAssignment(brokers: Seq[BrokerSelect])
case class GenerateMultipleAssignments(topics: Seq[TopicSelect], brokers: Seq[BrokerSelect])
case class RunMultipleAssignments(topics: Seq[TopicSelect])
case object RunAssignment extends ReassignPartitionOperation
case class UnknownRPO(op: String) extends ReassignPartitionOperation

object ReassignPartitionOperation {
  def apply(s: String) : ReassignPartitionOperation = {
    s match {
      case "run" => RunAssignment
      case a => UnknownRPO(a)
    }
  }

  def unapply(op: ReassignPartitionOperation) : Option[String] = {
    op match {
      case RunAssignment => Option("run")
      case UnknownRPO(_) => Option("unknown")
    }
  }
}
