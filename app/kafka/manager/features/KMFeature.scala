/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager.features

import kafka.manager.{Kafka_0_8_1_1, ClusterConfig}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.util.{Success, Failure, Try}

/**
 * Created by hiral on 8/22/15.
 */

trait KMFeature

sealed trait ClusterFeature extends KMFeature

case object KMLogKafkaFeature extends ClusterFeature
case object KMDeleteTopicFeature extends ClusterFeature
case object KMJMXMetricsFeature extends ClusterFeature

object ClusterFeature {
  private lazy val log = LoggerFactory.getLogger(classOf[ClusterFeature])
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
        log.error(s"Unknown application feature $s")
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
    
    if(clusterConfig.version != Kafka_0_8_1_1)
      buffer+=KMDeleteTopicFeature

    ClusterFeatures(buffer.toSet)
  }
}
