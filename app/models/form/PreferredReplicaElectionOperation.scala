/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package models.form

/**
 * @author hiral
 */
sealed trait PreferredReplicaElectionOperation

case object RunElection extends PreferredReplicaElectionOperation
case class UnknownPREO(op: String) extends PreferredReplicaElectionOperation

object PreferredReplicaElectionOperation {
  def apply(s: String) : PreferredReplicaElectionOperation = {
    s match {
      case "run" => RunElection
      case a => UnknownPREO(a)
    }
  }

  def unapply(op: PreferredReplicaElectionOperation) : Option[String] = {
    op match {
      case RunElection => Some("run")
      case UnknownPREO(_) => None
    }
  }

}
