/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager.features

import grizzled.slf4j.Logging
import kafka.manager.model.{ClusterConfig, Kafka_0_8_1_1}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
 * Created by hiral on 8/22/15.
 */

trait KMFeature

sealed trait ClusterFeature extends KMFeature

case object KMLogKafkaFeature extends ClusterFeature
case object KMDeleteTopicFeature extends ClusterFeature
case object KMJMXMetricsFeature extends ClusterFeature
case object KMDisplaySizeFeature extends ClusterFeature
case object KMPollConsumersFeature extends ClusterFeature

object ClusterFeature extends Logging {
  import scala.reflect.runtime.universe

  val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)

  def from(s: String) : Option[ClusterFeature] = {
    Try {
          val clazz = s"features.$s"
          val module = runtimeMirror.staticModule(clazz)
          val obj = runtimeMirror.reflectModule(module)
          obj.instance match {
            case f: ClusterFeature =>
              f
            case _ =>
              throw new IllegalArgumentException(s"Unknown application feature $s")
          }
        } match {
      case Failure(t) =>
        error(s"Unknown application feature $s")
        None
      case Success(f) => Option(f)
    }
  }

}

case class ClusterFeatures(features: Set[ClusterFeature])

object ClusterFeatures {
  val default = ClusterFeatures(Set())
  
  def from(clusterConfig: ClusterConfig) : ClusterFeatures = {
    val buffer = new ListBuffer[ClusterFeature]
    
    if(clusterConfig.logkafkaEnabled)
      buffer+=KMLogKafkaFeature

    if(clusterConfig.jmxEnabled)
      buffer+=KMJMXMetricsFeature

    if(clusterConfig.displaySizeEnabled)
      buffer+=KMDisplaySizeFeature
    
    if(clusterConfig.version != Kafka_0_8_1_1)
      buffer+=KMDeleteTopicFeature

    if(clusterConfig.pollConsumers)
      buffer+=KMPollConsumersFeature

    ClusterFeatures(buffer.toSet)
  }
}
