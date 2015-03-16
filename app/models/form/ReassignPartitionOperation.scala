/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package models.form

/**
 * @author hiral
 */
sealed trait ReassignPartitionOperation

case object ConfirmAssignment extends ReassignPartitionOperation
case class GenerateAssignment(brokers: Seq[Int]) extends ReassignPartitionOperation
case object RunAssignment extends ReassignPartitionOperation
case class UnknownRPO(op: String) extends ReassignPartitionOperation

object ReassignPartitionOperation {
  def apply(s: String, brokers: Seq[Int]) : ReassignPartitionOperation = {
    s match {
      case "confirm" => ConfirmAssignment
      case "generate" => GenerateAssignment(brokers)
      case "run" => RunAssignment
      case a => UnknownRPO(a)
    }
  }

  def unapply(op: ReassignPartitionOperation) : Option[(String, Seq[Int])] = {
    op match {
      case ConfirmAssignment => Some(("confirm", Nil))
      case GenerateAssignment(brokers) => Some(("generate", brokers))
      case RunAssignment => Some(("run", Nil))
      case UnknownRPO(_) => Some(("unknown", Nil))
    }
  }
}
