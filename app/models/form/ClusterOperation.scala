/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package models.form

import kafka.manager.ClusterConfig

/**
 * @author hiral
 */
sealed trait Operation
case object Enable extends Operation
case object Disable extends Operation
case object Delete extends Operation
case object Update extends Operation
case class Unknown(operation: String) extends Operation

object Operation {
  implicit def fromString(s:String) : Operation = {
    s match {
      case "Enable" => Enable
      case "Disable" => Disable
      case "Delete" => Delete
      case "Update" => Update
      case a: Any => Unknown(a.toString)
    }
  }
}

object ClusterOperation {
  def apply(operation: String,
            name: String,
            version: String,
            zkHosts: String,
            zkMaxRetry: Int,
            jmxEnabled: Boolean,
            jmxUser: String,
            jmxPass: String,
            pollConsumers: Boolean,
            filterConsumers: Boolean,
            logkafkaEnabled: Boolean, 
            activeOffsetCacheEnabled: Boolean,
            displaySizeEnabled: Boolean): ClusterOperation = {
    ClusterOperation(operation,ClusterConfig(name, version, zkHosts, zkMaxRetry, jmxEnabled, jmxUser, jmxPass,
      pollConsumers, filterConsumers, logkafkaEnabled, activeOffsetCacheEnabled, displaySizeEnabled))
  }

  def customUnapply(co: ClusterOperation) : Option[(String, String, String, String, Int, Boolean, String, String, Boolean, Boolean, Boolean, Boolean, Boolean)] = {
    Option((co.op.toString, co.clusterConfig.name, co.clusterConfig.version.toString,
            co.clusterConfig.curatorConfig.zkConnect, co.clusterConfig.curatorConfig.zkMaxRetry,
            co.clusterConfig.jmxEnabled, co.clusterConfig.jmxUser, co.clusterConfig.jmxPass,
            co.clusterConfig.pollConsumers, co.clusterConfig.filterConsumers, co.clusterConfig.logkafkaEnabled,
            co.clusterConfig.activeOffsetCacheEnabled, co.clusterConfig.displaySizeEnabled))
  }
}

case class ClusterOperation private(op: Operation, clusterConfig: ClusterConfig)


