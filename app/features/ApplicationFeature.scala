/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package features

import com.typesafe.config.Config
import grizzled.slf4j.Logging
import kafka.manager.features.KMFeature

import scala.util.{Failure, Success, Try}

sealed trait ApplicationFeature extends KMFeature

case object KMClusterManagerFeature extends ApplicationFeature
case object KMTopicManagerFeature extends ApplicationFeature
case object KMPreferredReplicaElectionFeature extends ApplicationFeature
case object KMScheduleLeaderElectionFeature extends ApplicationFeature
case object KMReassignPartitionsFeature extends ApplicationFeature
case object KMBootstrapClusterConfigFeature extends ApplicationFeature

object ApplicationFeature extends Logging {
  import scala.reflect.runtime.universe

  val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)

  def from(s: String) : Option[ApplicationFeature] = {
    Try {
      val clazz = s"features.$s"
      val module = runtimeMirror.staticModule(clazz)
      val obj = runtimeMirror.reflectModule(module)
      obj.instance match {
        case f: ApplicationFeature =>
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

case class ApplicationFeatures(features: Set[ApplicationFeature])

object ApplicationFeatures extends Logging {

  lazy val default : List[String] = List(
    KMClusterManagerFeature,
    KMTopicManagerFeature,
    KMPreferredReplicaElectionFeature, 
    KMReassignPartitionsFeature).map(_.getClass.getSimpleName)
  
  def getApplicationFeatures(config: Config) : ApplicationFeatures = {
    import scala.collection.JavaConverters._
    val configFeatures: Option[List[String]] = Try(config.getStringList("application.features").asScala.toList).toOption
    
    if(configFeatures.isEmpty) {
      warn(s"application.features not found in conf file, using default values $default")
    }

    val f = configFeatures.getOrElse(default).map(ApplicationFeature.from).flatten
    ApplicationFeatures(f.toSet)
  }
}
