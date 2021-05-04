/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.logkafka

import akka.actor.{ActorPath, Cancellable}
import kafka.manager.base.{LongRunningPoolActor, LongRunningPoolConfig}
import kafka.manager.features.KMLogKafkaFeature
import kafka.manager.model.ActorModel._
import kafka.manager.model.ClusterContext

import scala.concurrent.duration._
import scala.util.Try

/**
 * @author hiral
 */
case class LogkafkaViewCacheActorConfig(logkafkaStateActorPath: ActorPath, 
                                      clusterContext: ClusterContext,
                                      longRunningPoolConfig: LongRunningPoolConfig, 
                                      updatePeriod: FiniteDuration = 10 seconds)
class LogkafkaViewCacheActor(config: LogkafkaViewCacheActorConfig) extends LongRunningPoolActor {
  
  private[this] val ZERO = BigDecimal(0)

  private[this] var cancellable : Option[Cancellable] = None

  private[this] var logkafkaIdentities : Map[String, LogkafkaIdentity] = Map.empty

  private[this] var logkafkaConfigsOption : Option[LogkafkaConfigs] = None

  private[this] var logkafkaClientsOption : Option[LogkafkaClients] = None

  override def preStart() = {
    if (config.clusterContext.clusterFeatures.features(KMLogKafkaFeature)) {
      log.info("Started actor %s".format(self.path))
      log.info("Scheduling updater for %s".format(config.updatePeriod))
      cancellable = Some(
        context.system.scheduler.scheduleAtFixedRate(0 seconds,
          config.updatePeriod,
          self,
          LKVForceUpdate)(context.system.dispatcher,self)
      )
    }
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info("Stopped actor %s".format(self.path))
    log.info("Cancelling updater...")
    Try(cancellable.map(_.cancel()))
    super.postStop()
  }

  override protected def longRunningPoolConfig: LongRunningPoolConfig = config.longRunningPoolConfig

  override protected def longRunningQueueFull(): Unit = {
    log.error("Long running pool queue full, skipping!")
  }

  override def processActorRequest(request: ActorRequest): Unit = {
    request match {
      case LKVForceUpdate =>
        log.info("Updating logkafka view...")
        //ask for logkafka configs 
        val lastLogkafkaConfigsUpdateMillisOption: Option[Long] = logkafkaConfigsOption.map(_.lastUpdateMillis)
        context.actorSelection(config.logkafkaStateActorPath).tell(LKSGetAllLogkafkaConfigs(lastLogkafkaConfigsUpdateMillisOption), self)
        //ask for logkafka clients
        val lastLogkafkaClientsUpdateMillisOption: Option[Long] = logkafkaClientsOption.map(_.lastUpdateMillis)
        context.actorSelection(config.logkafkaStateActorPath).tell(LKSGetAllLogkafkaClients(lastLogkafkaClientsUpdateMillisOption), self)

      case LKVGetLogkafkaIdentities =>
        sender ! logkafkaIdentities
        
      case any: Any => log.warning("bvca : processActorRequest : Received unknown message: {}", any)
    }
  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case lcg: LogkafkaConfigs =>
        logkafkaConfigsOption = Some(lcg)
        updateView()

      case lct: LogkafkaClients =>
        logkafkaClientsOption = Some(lct)
        updateView()

      case any: Any => log.warning("bvca : processActorResponse : Received unknown message: {}", any)
    }
  }

  private[this] def updateView(): Unit = {
    for {
      logkafkaConfigs <- logkafkaConfigsOption
      logkafkaClients <- logkafkaClientsOption
    } {
      val lcgMap = Map(logkafkaConfigs.configs map { a => a.logkafka_id -> a }: _*)
      val lctMap = Map(logkafkaClients.clients map { a => a.logkafka_id -> a }: _*)
      logkafkaIdentities = lcgMap.map (kv =>
        kv._1 -> LogkafkaIdentity.from(kv._1, Some(kv._2), lctMap.get(kv._1)))
    }
  }
}
