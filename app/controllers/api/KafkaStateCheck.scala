/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers.api

import controllers.KafkaManagerContext
import features.ApplicationFeatures
import kafka.manager.model.ActorModel.{KMClusterList, TopicIdentity}
import models.navigation.Menus
import play.api.i18n.{I18nSupport, MessagesApi}
import play.api.libs.json._
import play.api.mvc._
import org.json4s.jackson.Serialization


/**
 * @author jisookim0513
 */

class KafkaStateCheck (val messagesApi: MessagesApi, val kafkaManagerContext: KafkaManagerContext)
                      (implicit af: ApplicationFeatures, menus: Menus) extends Controller with I18nSupport {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = kafkaManagerContext.getKafkaManager

  def brokers(c: String) = Action.async { implicit request =>
    kafkaManager.getBrokerList(c).map { errorOrBrokerList =>
      errorOrBrokerList.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        brokerList => Ok(Json.obj("brokers" -> brokerList.list.map(bi => bi.id).sorted))
      )
    }
  }

  def topics(c: String) = Action.async { implicit request =>
    kafkaManager.getTopicList(c).map { errorOrTopicList =>
      errorOrTopicList.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        topicList => Ok(Json.obj("topics" -> topicList.list.sorted))
      )
    }
  }

  def topicIdentities(c: String) = Action.async { implicit request =>
    kafkaManager.getTopicListExtended(c).map { errorOrTopicList =>
      errorOrTopicList.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        topicIdentityList => Ok(getTopicIdentitiesListJson(topicIdentityList.list))
      )
    }
  }

  def getTopicIdentitiesListJson(topicIdentities: IndexedSeq[(String, Option[TopicIdentity])]) = {
    import org.json4s.scalaz.JsonScalaz._

    implicit val formats = org.json4s.DefaultFormats
    Serialization.writePretty("topicIdentities" -> (for {
      (tn, tiOpt) <- topicIdentities
      ti <- tiOpt
      } yield toJSON(tiOpt)))
  }

  def clusters = Action.async { implicit request =>
    implicit val formats = org.json4s.DefaultFormats
    kafkaManager.getClusterList.map { errorOrClusterList =>
      errorOrClusterList.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        clusterList => Ok(Serialization.writePretty("clusters" -> errorOrClusterList.getOrElse(None)))
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

