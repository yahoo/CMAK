/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import features.ApplicationFeatures
import play.api.mvc._

/**
 * @author cvcal
 */
object Consumer extends Controller{
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = KafkaManagerContext.getKafkaManager
  private[this] implicit val af: ApplicationFeatures = ApplicationFeatures.features

  def consumers(cluster: String) = Action.async {
    kafkaManager.getConsumerListExtended(cluster).map { errorOrConsumerList =>
      Ok(views.html.consumer.consumerList(cluster, errorOrConsumerList))
    }
  }

  def consumer(cluster: String, consumerGroup: String) = Action.async {
    kafkaManager.getConsumerIdentity(cluster,consumerGroup).map { errorOrConsumerIdentity =>
      Ok(views.html.consumer.consumerView(cluster,consumerGroup,errorOrConsumerIdentity))
    }
  }

  def consumerAndTopic(cluster: String, consumerGroup: String, topic: String) = Action.async {
    kafkaManager.getConsumedTopicState(cluster,consumerGroup,topic).map { errorOrConsumedTopicState =>
      Ok(views.html.consumer.consumedTopicView(cluster,consumerGroup,topic,errorOrConsumedTopicState))
    }
  }
}
