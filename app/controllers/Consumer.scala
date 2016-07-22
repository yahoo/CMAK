/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import features.ApplicationFeatures
import models.navigation.Menus
import play.api.i18n.{I18nSupport, MessagesApi}
import play.api.mvc._

/**
 * @author cvcal
 */
class Consumer (val messagesApi: MessagesApi, val kafkaManagerContext: KafkaManagerContext)
               (implicit af: ApplicationFeatures, menus: Menus) extends Controller with I18nSupport {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = kafkaManagerContext.getKafkaManager

  def consumers(cluster: String) = Action.async {
    kafkaManager.getConsumerListExtended(cluster).map { errorOrConsumerList =>
      Ok(views.html.consumer.consumerList(cluster, errorOrConsumerList))
    }
  }

  def consumer(cluster: String, consumerGroup: String, consumerType: String) = Action.async {
    kafkaManager.getConsumerIdentity(cluster,consumerGroup, consumerType).map { errorOrConsumerIdentity =>
      Ok(views.html.consumer.consumerView(cluster,consumerGroup,errorOrConsumerIdentity))
    }
  }

  def consumerAndTopic(cluster: String, consumerGroup: String, topic: String, consumerType: String) = Action.async {
    kafkaManager.getConsumedTopicState(cluster,consumerGroup,topic, consumerType).map { errorOrConsumedTopicState =>
      Ok(views.html.consumer.consumedTopicView(cluster,consumerGroup,consumerType,topic,errorOrConsumedTopicState))
    }
  }
}
