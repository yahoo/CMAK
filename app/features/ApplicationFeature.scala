/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package features

import com.typesafe.config.Config
import kafka.manager.features.KMFeature
import org.slf4j.LoggerFactory

import scala.util.{Success, Failure, Try}

sealed trait ApplicationFeature extends KMFeature

case object KMClusterManagerFeature extends ApplicationFeature
case object KMTopicManagerFeature extends ApplicationFeature
case object KMPreferredReplicaElectionFeature extends ApplicationFeature
case object KMReassignPartitionsFeature extends ApplicationFeature
case object KMBootstrapClusterConfigFeature extends ApplicationFeature

object ApplicationFeature {
  private lazy val log = LoggerFactory.getLogger(classOf[ApplicationFeature])
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
        log.error(s"Unknown application feature $s")
        None
      case Success(f) => Option(f)
    }
  }
  
}

case class ApplicationFeatures(features: Set[ApplicationFeature])

object ApplicationFeatures {
  import play.api.Play.current
  private lazy val log = LoggerFactory.getLogger(classOf[ApplicationFeatures])
  
  lazy val default : List[String] = List(
    KMClusterManagerFeature,
    KMTopicManagerFeature,
    KMPreferredReplicaElectionFeature, 
    KMReassignPartitionsFeature).map(_.getClass.getSimpleName)
  
  lazy val features = {
    getApplicationFeatures(play.api.Play.configuration.underlying)
  }
  
  def getApplicationFeatures(config: Config) : ApplicationFeatures = {
    import scala.collection.JavaConverters._
    val configFeatures: Option[List[String]] = Try(config.getStringList("application.features").asScala.toList).toOption
    
    if(configFeatures.isEmpty) {
      log.warn(s"application.features not found in conf file, using default values $default")
    }

    val f = configFeatures.getOrElse(default).map(ApplicationFeature.from).flatten
    ApplicationFeatures(f.toSet)
  }
}
