/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package scheduler.kafka.manager

import akka.pattern.pipe
import kafka.manager.ActorModel._
import kafka.manager.{BaseQueryCommandActor, SchedulerConfig}
import org.apache.curator.framework.CuratorFramework

class KafkaSchedulerStateActor(curator: CuratorFramework,
                      schedulerConfig: SchedulerConfig) extends BaseQueryCommandActor {

  val schedulerRestClient = new SchedulerRestClient(schedulerConfig.apiUrl)(play.api.libs.concurrent.Execution.Implicits.defaultContext)

  @scala.throws[Exception](classOf[Exception])
  override def preStart() = {
    log.info("Started actor %s".format(self.path))
  }

  @scala.throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]) {
    log.error(reason, "Restarting due to [{}] when processing [{}]",
      reason.getMessage, message.getOrElse(""))
    super.preRestart(reason, message)
  }


  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info("Stopped actor %s".format(self.path))

    super.postStop()
  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case any: Any => log.warning("kssa : processActorResponse : Received unknown message: {}", any.toString)
    }
  }

  override def processQueryRequest(request: QueryRequest): Unit = {
    request match {
      case KSGetTopics =>
        sender ! TopicList(IndexedSeq(), Set(), null)

      case KSGetAllTopicDescriptions(lastUpdateMillisOption) =>
        sender ! TopicDescriptions(IndexedSeq.empty, 0L)

      case SchedulerKSGetBrokers =>
        implicit val ec = context.dispatcher

        def deserMap(map: String): Seq[(String, String)] =
          map.split(",").map {
            kv =>
              val pair = kv.split("=")
              (pair(0), pair(1))
          }

        schedulerRestClient.getStatus map {
          status =>
            val brokerIdentities =
            status.brokers.getOrElse(Seq.empty).map{
              b =>
                SchedulerBrokerIdentity(b.id.toInt, b.active, b.cpus, b.mem, b.heap, b.port,
                  b.bindAddress, b.constraints.map(deserMap).getOrElse(Seq.empty),
                  b.options.map(deserMap).getOrElse(Seq.empty), b.log4jOptions.map(deserMap).getOrElse(Seq.empty), b.jvmOptions,
                  SchedulerBrokerStickinessIdentity(b.stickiness.period, b.stickiness.stopTime, b.stickiness.hostname),
                  SchedulerBrokerFailoverIdentity(b.failover.delay, b.failover.maxDelay, b.failover.maxTries, b.failover.failures, b.failover.failureTime),
                  b.task.map(task => SchedulerBrokerTaskIdentity(task.id, task.slaveId, task.executorId, task.hostname, task.endpoint, task.state)), schedulerConfig)
            }
            SchedulerBrokerList(brokerIdentities, schedulerConfig)
        } pipeTo sender()

      case any: Any => log.warning("kssa : processQueryRequest : Received unknown message: {}", any.toString)
    }
  }

  override def processCommandRequest(request: CommandRequest): Unit = {
    request match {
      case any: Any => log.warning("kssa : processCommandRequest : Received unknown message: {}", any.toString)
    }
  }
}