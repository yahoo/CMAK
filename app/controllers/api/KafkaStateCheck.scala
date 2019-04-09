/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers.api

import controllers.KafkaManagerContext
import features.ApplicationFeatures
import kafka.manager.model.ActorModel.BrokerIdentity
import kafka.manager.model.SecurityProtocol
import models.navigation.Menus
import org.json4s.jackson.Serialization
import org.json4s.scalaz.JsonScalaz.toJSON
import play.api.i18n.I18nSupport
import play.api.mvc._

import scala.concurrent.{ExecutionContext, Future}

/**
 * @author jisookim0513
 */

class KafkaStateCheck (val cc: ControllerComponents, val kafkaManagerContext: KafkaManagerContext)
                      (implicit af: ApplicationFeatures, menus: Menus, ec:ExecutionContext) extends AbstractController(cc) with I18nSupport {

  private[this] val kafkaManager = kafkaManagerContext.getKafkaManager

  import play.api.libs.json._

  implicit val endpointMapWrites = new Writes[Map[SecurityProtocol, Int]] {
    override def writes(o: Map[SecurityProtocol, Int]): JsValue = Json.obj(
      "endpoints" -> o.toSeq.map(tpl => s"${tpl._1.stringId}:${tpl._2}")
    )
  }

  implicit val brokerIdentityWrites = Json.writes[BrokerIdentity]

  def brokers(c: String) = Action.async { implicit request:RequestHeader =>
    kafkaManager.getBrokerList(c).map { errorOrBrokerList =>
      errorOrBrokerList.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        brokerList => Ok(Json.obj("brokers" -> brokerList.list.map(bi => bi.id).sorted)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
      )
    }
  }
  def brokersExtended(c: String) = Action.async { implicit request:RequestHeader =>
    kafkaManager.getBrokerList(c).map { errorOrBrokerList =>
      errorOrBrokerList.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        brokerList => Ok(Json.obj("brokers" -> brokerList.list)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
      )
    }
  }

  def topics(c: String) = Action.async { implicit request:RequestHeader =>
    kafkaManager.getTopicList(c).map { errorOrTopicList =>
      errorOrTopicList.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        topicList => Ok(Json.obj("topics" -> topicList.list.sorted)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
      )
    }
  }

  def topicIdentities(c: String) = Action.async { implicit request:RequestHeader =>
    implicit val formats = org.json4s.DefaultFormats
    kafkaManager.getTopicListExtended(c).map { errorOrTopicListExtended =>
      errorOrTopicListExtended.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        topicListExtended => Ok(Serialization.writePretty("topicIdentities" -> topicListExtended.list.flatMap(_._2).map(toJSON(_)))).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
      )
    }
  }

  def clusters = Action.async { implicit request:RequestHeader =>
    implicit val formats = org.json4s.DefaultFormats
    kafkaManager.getClusterList.map { errorOrClusterList =>
      errorOrClusterList.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        clusterList => Ok(Serialization.writePretty("clusters" -> errorOrClusterList.toOption)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
      )
    }
  }

  def underReplicatedPartitions(c: String, t: String) = Action.async { implicit request:RequestHeader =>
    kafkaManager.getTopicIdentity(c, t).map { errorOrTopicIdentity =>
      errorOrTopicIdentity.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        topicIdentity => Ok(Json.obj("topic" -> t, "underReplicatedPartitions" -> topicIdentity.partitionsIdentity.filter(_._2.isUnderReplicated).map{case (num, pi) => pi.partNum})).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
      )
    }
  }

  def unavailablePartitions(c: String, t: String) = Action.async { implicit request:RequestHeader =>
    kafkaManager.getTopicIdentity(c, t).map { errorOrTopicIdentity =>
      errorOrTopicIdentity.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        topicIdentity => Ok(Json.obj("topic" -> t, "unavailablePartitions" -> topicIdentity.partitionsIdentity.filter(_._2.isr.isEmpty).map { case (num, pi) => pi.partNum })).withHeaders("X-Frame-Options" -> "SAMEORIGIN"))
    }
  }

  def topicSummaryAction(cluster: String, consumer: String, topic: String, consumerType: String) = Action.async { implicit request:RequestHeader =>
    getTopicSummary(cluster, consumer, topic, consumerType).map { errorOrTopicSummary =>
      errorOrTopicSummary.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        topicSummary => {
          Ok(topicSummary).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
        })
    }
  }

  def getTopicSummary(cluster: String, consumer: String, topic: String, consumerType: String) = {
    kafkaManager.getConsumedTopicState(cluster, consumer, topic, consumerType).map { errorOrTopicSummary =>
      errorOrTopicSummary.map(
        topicSummary => {
          def sortByPartition[T](f: Map[Int, T]) : Seq[(Int, T)] = {
            f.toSeq.sortBy { case (pnum, offset) => pnum }
          };

          Json.obj("totalLag" -> topicSummary.totalLag, "percentageCovered" -> topicSummary.percentageCovered, "partitionOffsets" -> sortByPartition(topicSummary.partitionOffsets).map {case (pnum, offset) => offset}, "partitionLatestOffsets" -> sortByPartition(topicSummary.partitionLatestOffsets).map {case (pnum, latestOffset) => latestOffset}, "owners" -> sortByPartition(topicSummary.partitionOwners).map {case (pnum, owner) => owner}
          )
        })
    }
  }

  def groupSummaryAction(cluster: String, consumer: String, consumerType: String) = Action.async { implicit request:RequestHeader =>
    kafkaManager.getConsumerIdentity(cluster, consumer, consumerType).flatMap { errorOrConsumedTopicSummary =>
      errorOrConsumedTopicSummary.fold(
        error =>
          Future.successful(BadRequest(Json.obj("msg" -> error.msg))),
        consumedTopicSummary => getGroupSummary(cluster, consumer, consumedTopicSummary.topicMap.keys, consumerType).map { topics =>
          Ok(JsObject(topics)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
        })
    }
  }

  def getGroupSummary(cluster: String, consumer: String, groups: Iterable[String], consumerType: String): Future[Map[String, JsObject]] = {
    val cosumdTopicSummary: List[Future[(String, JsObject)]] = groups.toList.map { group =>
      getTopicSummary(cluster, consumer, group, consumerType)
        .map(topicSummary => group -> topicSummary.getOrElse(Json.obj()))
    }
    Future.sequence(cosumdTopicSummary).map(_.toMap)
  }
  
  def consumersSummaryAction(cluster: String) = Action.async { implicit request:RequestHeader =>
    implicit val formats = org.json4s.DefaultFormats
    kafkaManager.getConsumerListExtended(cluster).map { errorOrConsumersSummary =>
      errorOrConsumersSummary.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        consumersSummary =>
          Ok(Serialization.writePretty("consumers" ->
            consumersSummary.list.map {
              case ((consumer, consumerType), consumerIdentity) =>
                Map("name" -> consumer,
                  "type" -> consumerType.toString,
                  "topics" -> consumerIdentity.map(_.topicMap.keys),
                  "lags" -> consumerIdentity.map(_.topicMap.mapValues(_.totalLag))
                )
            })).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
        )
    }
  }

}
