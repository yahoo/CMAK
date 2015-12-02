/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
import features.{ApplicationFeatures, ApplicationFeature}
import kafka.manager.model.ClusterContext
import kafka.manager.{KafkaManager, ApiError}
import kafka.manager.features.{ClusterFeatures, ClusterFeature}
import play.api.mvc._
import play.api.mvc.Results._

import scala.concurrent.Future
import scalaz.{\/-, -\/, \/}

/**
 * Created by hiral on 8/23/15.
 */
package object controllers {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  def featureGate(af: ApplicationFeature)(fn: => Future[Result])(implicit features: ApplicationFeatures) : Future[Result] = {
    if(features.features(af)) {
      fn
    } else {
      Future.successful(Ok(views.html.errors.onApiError(ApiError(s"Feature disabled $af"))))
    }
  }
  
  def clusterFeatureGate(clusterName: String, cf: ClusterFeature)(fn: ClusterContext => Future[Result])
           (implicit km: KafkaManager) : Future[Result] = {
    km.getClusterContext(clusterName).flatMap { clusterContextOrError =>
      clusterContextOrError.fold( 
        error => {
          Future.successful(Ok(views.html.errors.onApiError(error, None)))
        }, 
        clusterContext => {
          if(clusterContext.clusterFeatures.features(cf)) {
            fn(clusterContext)
          } else {
            Future.successful(Ok(views.html.errors.onApiError(ApiError(s"Unsupported feature : $cf"), None)))
          }
        })
    }.recover {
      case t =>
        Ok(views.html.errors.onApiError(ApiError(t.getMessage), None))
    }
  }
  
  def withClusterFeatures(clusterName: String)(err: ApiError => Future[Result], fn: ClusterFeatures => Future[Result])
                         (implicit km: KafkaManager) : Future[Result] = {
    km.getClusterContext(clusterName).flatMap { clusterContextOrError =>
      clusterContextOrError.map(_.clusterFeatures) match {
        case -\/(error) => err(error)
        case \/-(f) => fn(f)
      }
    }
  }

  def withClusterContext(clusterName: String)(err: ApiError => Future[Result], fn: ClusterContext => Future[Result])
                         (implicit km: KafkaManager) : Future[Result] = {
    km.getClusterContext(clusterName).flatMap { clusterContextOrError =>
      clusterContextOrError match {
        case -\/(error) => err(error)
        case \/-(f) => fn(f)
      }
    }
  }
}
