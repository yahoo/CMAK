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

    def monitoring(c: String) = Action.async { implicit request:RequestHeader =>
      kafkaManager.getBrokerList(c).map { errorOrBrokerList =>
        errorOrBrokerList.fold(
          error => BadRequest(Json.obj("msg" -> error.msg)),
          brokerList => Ok(Json.obj("messagesInSecMean" -> brokerList.combinedMetric.map(_.messagesInPerSec.formatMeanRate),
            "messagesInSecMean" -> brokerList.combinedMetric.map(_.messagesInPerSec.formatMeanRate),
            "messagesInSecOneMin" -> brokerList.combinedMetric.map(_.messagesInPerSec.formatOneMinuteRate),
            "messagesInSecFiveMin" -> brokerList.combinedMetric.map(_.messagesInPerSec.formatFiveMinuteRate),
            "messagesInSecFifteenMin" -> brokerList.combinedMetric.map(_.messagesInPerSec.formatFifteenMinuteRate),
            "bytesInSec" -> brokerList.combinedMetric.map(_.bytesInPerSec.formatMeanRate),
            "bytesInSecOneMin" -> brokerList.combinedMetric.map(_.bytesInPerSec.formatOneMinuteRate),
            "bytesInSecFiveMin" -> brokerList.combinedMetric.map(_.bytesInPerSec.formatFiveMinuteRate),
            "bytesInSecFifteenMin" -> brokerList.combinedMetric.map(_.bytesInPerSec.formatFifteenMinuteRate),
            "bytesOutSec" -> brokerList.combinedMetric.map(_.bytesOutPerSec.formatMeanRate),
            "bytesOutSecOneMin" -> brokerList.combinedMetric.map(_.bytesOutPerSec.formatOneMinuteRate),
            "bytesOutSecFiveMin" -> brokerList.combinedMetric.map(_.bytesOutPerSec.formatFiveMinuteRate),
            "bytesOutSecFifteenMin" -> brokerList.combinedMetric.map(_.bytesOutPerSec.formatFifteenMinuteRate),
            "bytesRejectSec" -> brokerList.combinedMetric.map(_.bytesRejectedPerSec.formatMeanRate),
            "bytesRejectSecOneMin" -> brokerList.combinedMetric.map(_.bytesRejectedPerSec.formatOneMinuteRate),
            "bytesRejectSecFiveMin" -> brokerList.combinedMetric.map(_.bytesRejectedPerSec.formatFiveMinuteRate),
            "bytesRejectSecFifteenMin" -> brokerList.combinedMetric.map(_.bytesRejectedPerSec.formatFifteenMinuteRate),
            "failedFetchRequestSec" -> brokerList.combinedMetric.map(_.failedFetchRequestsPerSec.formatMeanRate),
            "failedFetchRequestSecOneMin" -> brokerList.combinedMetric.map(_.failedFetchRequestsPerSec.formatOneMinuteRate),
            "failedFetchRequestSecFiveMin" -> brokerList.combinedMetric.map(_.failedFetchRequestsPerSec.formatFiveMinuteRate),
            "failedFetchRequestSecFifteenMin" -> brokerList.combinedMetric.map(_.failedFetchRequestsPerSec.formatFifteenMinuteRate),
            "failedProduceRequestSec" -> brokerList.combinedMetric.map(_.failedProduceRequestsPerSec.formatMeanRate),
            "failedProduceRequestSecOneMin" -> brokerList.combinedMetric.map(_.failedProduceRequestsPerSec.formatOneMinuteRate),
            "failedProduceRequestSecFiveMin" -> brokerList.combinedMetric.map(_.failedProduceRequestsPerSec.formatFiveMinuteRate),
            "failedProduceRequestSecFifteenMin" -> brokerList.combinedMetric.map(_.failedProduceRequestsPerSec.formatFifteenMinuteRate))
          ).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
        )
      }
    }

    def brokermonitoring(c: String, b: Int) = Action.async { implicit request:RequestHeader =>
      implicit val formats = org.json4s.DefaultFormats
      kafkaManager.getBrokerView(c,b).map { errorOrBrokerList =>
        errorOrBrokerList.fold(
          error => BadRequest(Json.obj("msg" -> error.msg)),
          brokerView => Ok(Json.obj("numTopics" -> brokerView.numTopics,
            "numPartitions" -> brokerView.numPartitions,
            "numPartitionsAsLeader" -> brokerView.numPartitionsAsLeader,
            "perMessages" -> brokerView.stats.map(_.perMessages),
            "perIncoming" -> brokerView.stats.map(_.perIncoming),
            "perOutgoing" -> brokerView.stats.map(_.perOutgoing),
            "messagesCount" -> brokerView.messagesPerSecCountHistory.map(v => v.last.count),
            "messagesInSecMean" -> brokerView.metrics.map(_.messagesInPerSec.formatMeanRate),
            "messagesInSecOneMin" -> brokerView.metrics.map(_.messagesInPerSec.formatOneMinuteRate),
            "messagesInSecFiveMin" -> brokerView.metrics.map(_.messagesInPerSec.formatFiveMinuteRate),
            "messagesInSecFifteenMin" -> brokerView.metrics.map(_.messagesInPerSec.formatFifteenMinuteRate),
            "bytesInSec" -> brokerView.metrics.map(_.bytesInPerSec.formatMeanRate),
            "bytesInSecOneMin" -> brokerView.metrics.map(_.bytesInPerSec.formatOneMinuteRate),
            "bytesInSecFiveMin" -> brokerView.metrics.map(_.bytesInPerSec.formatFiveMinuteRate),
            "bytesInSecFifteenMin" -> brokerView.metrics.map(_.bytesInPerSec.formatFifteenMinuteRate),
            "bytesOutSec" -> brokerView.metrics.map(_.bytesOutPerSec.formatMeanRate),
            "bytesOutSecOneMin" -> brokerView.metrics.map(_.bytesOutPerSec.formatOneMinuteRate),
            "bytesOutSecFiveMin" -> brokerView.metrics.map(_.bytesOutPerSec.formatFiveMinuteRate),
            "bytesOutSecFifteenMin" -> brokerView.metrics.map(_.bytesOutPerSec.formatFifteenMinuteRate),
            "bytesRejectSec" -> brokerView.metrics.map(_.bytesRejectedPerSec.formatMeanRate),
            "bytesRejectSecOneMin" -> brokerView.metrics.map(_.bytesRejectedPerSec.formatOneMinuteRate),
            "bytesRejectSecFiveMin" -> brokerView.metrics.map(_.bytesRejectedPerSec.formatFiveMinuteRate),
            "bytesRejectSecFifteenMin" -> brokerView.metrics.map(_.bytesRejectedPerSec.formatFifteenMinuteRate),
            "failedFetchRequestSec" -> brokerView.metrics.map(_.failedFetchRequestsPerSec.formatMeanRate),
            "failedFetchRequestSecOneMin" -> brokerView.metrics.map(_.failedFetchRequestsPerSec.formatOneMinuteRate),
            "failedFetchRequestSecFiveMin" -> brokerView.metrics.map(_.failedFetchRequestsPerSec.formatFiveMinuteRate),
            "failedFetchRequestSecFifteenMin" -> brokerView.metrics.map(_.failedFetchRequestsPerSec.formatFifteenMinuteRate),
            "failedProduceRequestSec" -> brokerView.metrics.map(_.failedProduceRequestsPerSec.formatMeanRate),
            "failedProduceRequestSecOneMin" -> brokerView.metrics.map(_.failedProduceRequestsPerSec.formatOneMinuteRate),
            "failedProduceRequestSecFiveMin" -> brokerView.metrics.map(_.failedProduceRequestsPerSec.formatFiveMinuteRate),
            "failedProduceRequestSecFifteenMin" -> brokerView.metrics.map(_.failedProduceRequestsPerSec.formatFifteenMinuteRate)
          )).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
          )
      }
    }

}
