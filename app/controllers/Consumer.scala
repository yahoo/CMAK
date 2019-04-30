/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import features.ApplicationFeatures
import models.navigation.Menus
import play.api.i18n.I18nSupport
import play.api.mvc._

import scala.concurrent.ExecutionContext

/**
 * @author cvcal
 */
class Consumer (val cc: ControllerComponents, val kafkaManagerContext: KafkaManagerContext)
               (implicit af: ApplicationFeatures, menus: Menus, ec: ExecutionContext) extends AbstractController(cc) with I18nSupport {

  private[this] val kafkaManager = kafkaManagerContext.getKafkaManager

  def consumers(cluster: String) = Action.async { implicit request: RequestHeader =>
    kafkaManager.getConsumerListExtended(cluster).map { errorOrConsumerList =>
      Ok(views.html.consumer.consumerList(cluster, errorOrConsumerList)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
    }
  }

  def consumer(cluster: String, consumerGroup: String, consumerType: String) = Action.async { implicit request: RequestHeader =>
    kafkaManager.getConsumerIdentity(cluster,consumerGroup, consumerType).map { errorOrConsumerIdentity =>
      Ok(views.html.consumer.consumerView(cluster,consumerGroup,errorOrConsumerIdentity)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
    }
  }

  def consumerAndTopic(cluster: String, consumerGroup: String, topic: String, consumerType: String) = Action.async { implicit request: RequestHeader =>
    kafkaManager.getConsumedTopicState(cluster,consumerGroup,topic, consumerType).map { errorOrConsumedTopicState =>
      Ok(views.html.consumer.consumedTopicView(cluster,consumerGroup,consumerType,topic,errorOrConsumedTopicState)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
    }
  }
}
