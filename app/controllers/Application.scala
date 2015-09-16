/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import features.ApplicationFeatures
import kafka.manager.features.ClusterFeatures
import play.api.mvc._

/**
 * @author hiral
 */
object Application extends Controller {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = KafkaManagerContext.getKafkaManager

  private[this] implicit val af: ApplicationFeatures = ApplicationFeatures.features

  def index = Action.async {
    for {errorOrSchedulerList <- kafkaManager.getSchedulerList
         errorOrClusterList <- kafkaManager.getClusterList
    } yield Ok(views.html.index(errorOrClusterList, errorOrSchedulerList))
  }
}
