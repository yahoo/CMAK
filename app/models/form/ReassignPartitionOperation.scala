/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package models.form

/**
 * @author hiral
 */
sealed trait ReassignPartitionOperation

case object GenerateAssignment extends ReassignPartitionOperation
case object RunAssignment extends ReassignPartitionOperation
case class UnknownRPO(op: String) extends ReassignPartitionOperation

object ReassignPartitionOperation {
  def apply(s: String) : ReassignPartitionOperation = {
    s match {
      case "generate" => GenerateAssignment
      case "run" => RunAssignment
      case a => UnknownRPO(a)
    }
  }

  def unapply(op: ReassignPartitionOperation) : Option[String] = {
    op match {
      case GenerateAssignment => Some("generate")
      case RunAssignment => Some("run")
      case UnknownRPO(_) => None
    }
  }

}
