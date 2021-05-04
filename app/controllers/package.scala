/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
import features.{ApplicationFeature, ApplicationFeatures}
import kafka.manager.features.{ClusterFeature, ClusterFeatures}
import kafka.manager.model.ClusterContext
import kafka.manager.{ApiError, KafkaManager}
import play.api.mvc.Results._
import play.api.mvc._
import scalaz.{-\/, \/-}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by hiral on 8/23/15.
 */
package object controllers {

  def featureGate(af: ApplicationFeature)(fn: => Future[Result])(implicit features: ApplicationFeatures) : Future[Result] = {
    if(features.features(af)) {
      fn
    } else {
      Future.successful(Ok(views.html.errors.onApiError(ApiError(s"Feature disabled $af"))).withHeaders("X-Frame-Options" -> "SAMEORIGIN"))
    }
  }
  
  def clusterFeatureGate(clusterName: String, cf: ClusterFeature)(fn: ClusterContext => Future[Result])
           (implicit km: KafkaManager, ec:ExecutionContext) : Future[Result] = {
    km.getClusterContext(clusterName).flatMap { clusterContextOrError =>
      clusterContextOrError.fold( 
        error => {
          Future.successful(Ok(views.html.errors.onApiError(error, None)).withHeaders("X-Frame-Options" -> "SAMEORIGIN"))
        }, 
        clusterContext => {
          if(clusterContext.clusterFeatures.features(cf)) {
            fn(clusterContext)
          } else {
            Future.successful(Ok(views.html.errors.onApiError(ApiError(s"Unsupported feature : $cf"), None)).withHeaders("X-Frame-Options" -> "SAMEORIGIN"))
          }
        })
    }.recover {
      case t =>
        Ok(views.html.errors.onApiError(ApiError(t.getMessage), None)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
    }
  }
  
  def withClusterFeatures(clusterName: String)(err: ApiError => Future[Result], fn: ClusterFeatures => Future[Result])
                         (implicit km: KafkaManager, ec: ExecutionContext) : Future[Result] = {
    km.getClusterContext(clusterName).flatMap { clusterContextOrError =>
      clusterContextOrError.map(_.clusterFeatures) match {
        case -\/(error) => err(error)
        case \/-(f) => fn(f)
      }
    }
  }

  def withClusterContext(clusterName: String)(err: ApiError => Future[Result], fn: ClusterContext => Future[Result])
                         (implicit km: KafkaManager, ec: ExecutionContext) : Future[Result] = {
    km.getClusterContext(clusterName).flatMap { clusterContextOrError =>
      clusterContextOrError match {
        case -\/(error) => err(error)
        case \/-(f) => fn(f)
      }
    }
  }
}
