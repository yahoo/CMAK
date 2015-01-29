/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import akka.actor.{Cancellable, ActorPath}
import scala.concurrent.duration._
import scala.util.Try

/**
 * @author hiral
 */
import ActorModel._
class BrokerViewCacheActor(kafkaStateActorPath: ActorPath, updatePeriod: FiniteDuration = 10 seconds) extends BaseActor {

  private[this] var cancellable : Option[Cancellable] = None

  private[this] var brokerTopicPartitions : Map[Int,BVView] = Map.empty

  private[this] var topicDescriptionsOption : Option[TopicDescriptions] = None

  private[this] var brokerListOption : Option[BrokerList] = None

  override def preStart() = {
    log.info("Started actor %s".format(self.path))
    log.info("Scheduling updater for %s".format(updatePeriod))
    cancellable = Some(
      context.system.scheduler.schedule(0 seconds,
        updatePeriod,
        self,
        BVForceUpdate)(context.system.dispatcher,self)
    )
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info("Stopped actor %s".format(self.path))
    log.info("Cancelling updater...")
    Try(cancellable.map(_.cancel()))
  }

  override def processActorRequest(request: ActorRequest): Unit = {
    request match {
      case BVForceUpdate =>
        log.info("Updating broker view...")
        //ask for topic descriptions
        val lastUpdateMillisOption: Option[Long] = topicDescriptionsOption.map(_.lastUpdateMillis)
        context.actorSelection(kafkaStateActorPath).tell(KSGetAllTopicDescriptions(lastUpdateMillisOption), self)
        context.actorSelection(kafkaStateActorPath).tell(KSGetBrokers, self)

      case BVGetView(id) =>
        sender ! brokerTopicPartitions.get(id)

      case any: Any => log.warning("Received unknown message: {}", any)
    }
  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case td: TopicDescriptions =>
        topicDescriptionsOption = Some(td)
        updateView()

      case bl: BrokerList =>
        brokerListOption = Some(bl)
        updateView()

      case any: Any => log.warning("Received unknown message: {}", any)
    }
  }

  private[this] def updateView(): Unit = {
    for {
      brokerList <- brokerListOption
      topicDescriptions <- topicDescriptionsOption
    } {
      val topicIdentity : IndexedSeq[TopicIdentity] = topicDescriptions.descriptions.map(TopicIdentity.from(brokerList.list.size,_))
      val groupByBroker =
        topicIdentity.flatMap(ti => ti.partitionsByBroker.map( btp => (ti,btp.id,btp.partitions))).groupBy(_._2)

      brokerTopicPartitions=groupByBroker.map {
        case (brokerId, topicPartitions) =>
          val topicPartitionsMap : Map[TopicIdentity, IndexedSeq[Int]] = topicPartitions.map {
            case (topic, id, partitions) => (topic, partitions)
          }.toMap
          (brokerId, BVView(topicPartitionsMap))
      }.toMap
    }

  }

}
