package scheduler.kafka.manager

import java.util.Date

import org.slf4j.LoggerFactory
import play.api.Play.current
import play.api.libs.json.{JsError, JsSuccess}
import play.api.libs.ws._
import scheduler.kafka.manager.SchedulerRestClient.{AddBrokerResponse, Broker, StatusResponse}
import scheduler.models.form.Failover

import scala.concurrent.{ExecutionContext, Future}

object SchedulerRestClient {

  import play.api.libs.functional.syntax._
  import play.api.libs.json._


  case class Task(id: String,
                  slaveId: String,
                  executorId: String,
                  hostname: String,
                  endpoint: Option[String],
                  state: String)

  implicit val taskReads: Reads[Task] = (
    (__ \ 'id).read[String] and
      (__ \ 'slaveId).read[String] and
      (__ \ 'executorId).read[String] and
      (__ \ 'hostname).read[String] and
      (__ \ 'endpoint).readNullable[String] and
      (__ \ 'state).read[String]
    )(Task)

  case class Failover(delay: String,
                      maxDelay: String,
                      maxTries: Option[Int],
                      failures: Option[Int],
                      failureTime: Option[Date])

  implicit val failoverReads: Reads[Failover] = (
    (__ \ 'delay).read[String] and
      (__ \ 'maxDelay).read[String] and
      (__ \ 'maxTries).readNullable[Int] and
      (__ \ 'failures).readNullable[Int] and
      (__ \ 'failureTime).readNullable[Date]
    )(Failover)

  case class Stickiness(period: String, stopTime: Option[Date], hostname: Option[String])

  implicit val stickinessReads: Reads[Stickiness] = (
    (__ \ 'period).read[String] and
      (__ \ 'stopTime).readNullable[Date] and
      (__ \ 'hostname).readNullable[String]
    )(Stickiness)


  case class Broker(id: String,
                    active: Boolean,
                    cpus: Double,
                    mem: Long,
                    heap: Long,
                    port: Option[String],
                    bindAddress: Option[String],
                    constraints: Option[String],
                    options: Option[String],
                    log4jOptions: Option[String],
                    jvmOptions: Option[String],
                    stickiness: Stickiness,
                    failover: Failover,
                    task: Option[Task])

  implicit val brokerReads: Reads[Broker] = (
    (__ \ 'id).read[String] and
      (__ \ 'active).read[Boolean] and
      (__ \ 'cpus).read[Double] and
      (__ \ 'mem).read[Long] and
      (__ \ 'heap).read[Long] and
      (__ \ 'port).readNullable[String] and
      (__ \ 'bindAddress).readNullable[String] and
      (__ \ 'constraints).readNullable[String] and
      (__ \ 'options).readNullable[String] and
      (__ \ 'log4jOptions).readNullable[String] and
      (__ \ 'jvmOptions).readNullable[String] and
      (__ \ 'stickiness).read[Stickiness] and
      (__ \ 'failover).read[Failover] and
      (__ \ 'task).readNullable[Task]
    )(Broker)

  case class StatusResponse(brokers: Option[Seq[Broker]], frameworkId: Option[String])

  implicit val statusResponseReads: Reads[StatusResponse] = (
    (__ \ 'brokers).readNullable[Seq[Broker]] and
      (__ \ 'frameworkId).readNullable[String]
    )(StatusResponse)


  case class AddBrokerResponse(brokers: Seq[Broker])

  implicit val addBrokerResponseReads: Reads[AddBrokerResponse] =
    (__ \ 'brokers).read[Seq[Broker]].map(AddBrokerResponse)

}

class SchedulerRestClient(val apiUrl: String)(implicit val executionContext: ExecutionContext) {
  private[this] lazy val logger = LoggerFactory.getLogger(this.getClass)

  private val BrokerApiPrefix = s"$apiUrl/api/broker"
  private val StatusUrl = s"$BrokerApiPrefix/list"
  private val AddBrokerUrl = s"$BrokerApiPrefix/add"
  private val UpdateBrokerUrl = s"$BrokerApiPrefix/update"
  private val StartBrokerUrl = s"$BrokerApiPrefix/start"
  private val StopBrokerUrl = s"$BrokerApiPrefix/stop"
  private val RemoveBrokerUrl = s"$BrokerApiPrefix/remove"

  private val TopicApiPrefix = s"$apiUrl/api/topic"
  private val RebalanceTopicsUrl = s"$TopicApiPrefix/rebalance"

  private val Timeout = 10000

  def getStatus: Future[StatusResponse] = {
    val holder: Future[WSResponse] = WS
      .url(StatusUrl)
      .withRequestTimeout(Timeout)
      .get()

    holder.map {
      response => response.json.validate[StatusResponse]
    }.flatMap {
      case JsError(e) =>
        logger.error(s"Failed to parse status response $e")
        Future.failed(new Exception("Failed to parse status response json"))
      case JsSuccess(status, _) =>
        Future.successful(status)
    }
  }

  def addBroker(id: Int, cpus: Option[Double], mem: Option[Long], heap: Option[Long], port: Option[String],
                bindAddress: Option[String], constraints: Option[String], options: Option[String],
                log4jOptions: Option[String], jvmOptions: Option[String], stickinessPeriod: Option[String],
                failover: Failover): Future[Seq[Broker]] = {

    val queryParamsSeq = Seq(
      "broker" -> Some(id.toString), "cpus" -> cpus.map(_.toString), "mem" -> mem.map(_.toString), "heap" -> heap.map(_.toString),
      "port" -> port, "bindAddress" -> bindAddress, "constraints" -> constraints,
      "options" -> options, "log4jOptions" -> log4jOptions, "jvmOptions" -> jvmOptions,
      "stickinessPeriod" -> stickinessPeriod.map(_.toString), "failoverDelay" -> failover.failoverDelay.map(_.toString),
      "failoverMaxDelay" -> failover.failoverMaxDelay.map(_.toString), "failoverMaxTries" -> failover.failoverMaxTries.map(_.toString)).collect {
      case (key, Some(v)) => (key, v)
    }

    val holder: Future[WSResponse] = WS
      .url(AddBrokerUrl)
      .withQueryString(queryParamsSeq: _*)
      .withRequestTimeout(Timeout)
      .get()

    holder.map {
      response =>
        response.json.validate[AddBrokerResponse]
    }.flatMap {
      case JsError(e) =>
        logger.error(s"Failed to parse add broker response $e")
        Future.failed(new Exception("Failed to parse add broker response json"))
      case JsSuccess(brokers, _) =>
        Future.successful(brokers.brokers)
    }
  }

  def updateBroker(id: Int, cpus: Option[Double], mem: Option[Long], heap: Option[Long], port: Option[String],
                   bindAddress: Option[String], constraints: Option[String], options: Option[String],
                   log4jOptions: Option[String], jvmOptions: Option[String], stickinessPeriod: Option[String],
                   failover: Failover): Future[Unit] = {

    val queryParamsSeq = Seq(
      "broker" -> Some(id.toString), "cpus" -> cpus.map(_.toString), "mem" -> mem.map(_.toString), "heap" -> heap.map(_.toString),
      "port" -> port, "bindAddress" -> bindAddress, "constraints" -> constraints,
      "options" -> options, "log4jOptions" -> log4jOptions, "jvmOptions" -> jvmOptions,
      "stickinessPeriod" -> stickinessPeriod.map(_.toString), "failoverDelay" -> failover.failoverDelay.map(_.toString),
      "failoverMaxDelay" -> failover.failoverMaxDelay.map(_.toString), "failoverMaxTries" -> failover.failoverMaxTries.map(_.toString)).collect {
      case (key, Some(v)) => (key, v)
    }

    val holder: Future[WSResponse] = WS
      .url(UpdateBrokerUrl)
      .withQueryString(queryParamsSeq: _*)
      .withRequestTimeout(Timeout)
      .get()

    holder.map { _ => () }
  }

  def startBroker(id: Int): Future[Unit] = {
    val holder: Future[WSResponse] = WS
      .url(StartBrokerUrl)
      .withQueryString(Seq("broker" -> id.toString, "timeout" -> "0"): _*)
      .withRequestTimeout(Timeout)
      .get()

    holder.map { _ => () }
  }

  def stopBroker(id: Int): Future[Unit] = {
    val holder: Future[WSResponse] = WS
      .url(StopBrokerUrl)
      .withQueryString(Seq("broker" -> id.toString, "timeout" -> "0"): _*)
      .withRequestTimeout(Timeout)
      .get()

    holder.map { _ => () }
  }

  def removeBroker(id: Int): Future[Unit] = {
    val holder: Future[WSResponse] = WS
      .url(RemoveBrokerUrl)
      .withQueryString(Seq("broker" -> id.toString): _*)
      .withRequestTimeout(Timeout)
      .get()

    holder.map { _ => () }
  }

  def rebalanceTopics(ids: String, topics: Option[String]): Future[Unit] = {
    val holder: Future[WSResponse] = WS
      .url(RebalanceTopicsUrl)
      .withQueryString(Seq("broker" -> Some(ids), "topic" -> topics).collect {
      case (key, Some(v)) => (key, v)
    }: _*)
      .withRequestTimeout(Timeout)
      .get()

    holder.map { _ => () }
  }
}
