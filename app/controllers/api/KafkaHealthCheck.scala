/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers.api

import controllers.KafkaManagerContext
import play.api.libs.json._
import play.api.mvc._

object KafkaHealthCheck extends Controller {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = KafkaManagerContext.getKafkaManager

  def availableBrokers(c: String) = Action.async { implicit request =>
    kafkaManager.getBrokerList(c).map { errorOrBrokerList =>
      errorOrBrokerList.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        brokerList => Ok(Json.obj("availableBrokers" -> brokerList.list.map(bi => bi.id)))
      )
    }
  }

  def underReplicatedPartitions(c: String, t: String) = Action.async { implicit request =>
    kafkaManager.getTopicIdentity(c,t).map { errorOrTopicIdentity =>
      errorOrTopicIdentity.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        topicIdentity => Ok(Json.obj("topic" -> t, "underReplicatedPartitions" -> topicIdentity.partitionsIdentity.filter(_._2.isUnderReplicated).map{case (num, pi) => pi.partNum}))
      )
    }
  }

  def unavailablePartitions(c: String, t: String) = Action.async { implicit request =>
    kafkaManager.getTopicIdentity(c,t).map { errorOrTopicIdentity =>
      errorOrTopicIdentity.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        topicIdentity => Ok(Json.obj("topic" -> t, "unavailablePartitions" -> topicIdentity.partitionsIdentity.filter(_._2.isr.isEmpty).map{case (num, pi) => pi.partNum}))
      )
    }
  }
}

