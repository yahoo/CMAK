package controllers

import play.api.mvc._
import play.api.libs.json._

object KafkaHealthCheck extends Controller {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = KafkaManagerContext.getKafkaManger

  def availableBrokers(c: String) = Action.async {
    kafkaManager.getBrokerList(c).map { errorOrBrokerList =>
      errorOrBrokerList.fold(
        error => BadRequest(error.msg),
        brokerList => Ok(Json.obj("availableBrokers" -> brokerList.list.map(bi => bi.id)))
      )
    }
  }

  def underReplicatedPartitions(c: String, t: String) = Action.async {
    kafkaManager.getTopicIdentity(c,t).map { errorOrTopicIdentity =>
      errorOrTopicIdentity.fold(
        error => BadRequest(error.msg),
        topicIdentity => Ok(Json.obj("topic" -> t, "underReplicatedPartitions" -> topicIdentity.partitionsIdentity.filter(_._2.isUnderReplicated).map{case (num, pi) => pi.partNum}))
      )
    }
  }

  def unavailablePartitions(c: String, t: String) = Action.async {
    kafkaManager.getTopicIdentity(c,t).map { errorOrTopicIdentity =>
      errorOrTopicIdentity.fold(
        error => BadRequest(error.msg),
        topicIdentity => Ok(Json.obj("topic" -> t, "unavailablePartitions" -> topicIdentity.partitionsIdentity.filter(_._2.isr.isEmpty).map{case (num, pi) => pi.partNum}))
      )
    }
  }
}

