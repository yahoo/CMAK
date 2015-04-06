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

  private[this] var brokerTopicPartitions : Map[Int, BVView] = Map.empty

  private[this] var topicDescriptionsOption : Option[TopicDescriptions] = None

  private[this] var brokerListOption : Option[BrokerList] = None

  private[this] var brokerTopicMetrics : Map[(Int, String), BrokerMetrics] = Map.empty

  private[this] var brokerMetrics : Map[Int, BrokerMetrics] = Map.empty

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

      case BVGetTopicMetrics(id, topic) =>
        sender ! brokerTopicMetrics.get((id, topic))

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
      val topicIdentity : IndexedSeq[TopicIdentity] = topicDescriptions.descriptions.map(TopicIdentity.from(brokerList.list.size,_,None))
      val topicPartitionByBroker = topicIdentity.flatMap(ti => ti.partitionsByBroker.map(btp => (ti,btp.id,btp.partitions))).groupBy(_._2)
      // TODO Read the option in the cluster config to know if JMX metrics is on/off
      brokerTopicMetrics = topicPartitionByBroker.flatMap {
        case (brokerId, topicPartitions) =>
          val brokerInfoOpt = brokerList.list.find(_.id == brokerId)
          brokerInfoOpt.map {
            broker =>
              topicPartitions.map {
                case (topic, id, partitions) =>
                  ((brokerId, topic.topic), getBrokerMetrics(broker, Some(topic.topic)))
              }.toMap
          }
      }.flatten.toMap
      brokerMetrics = brokerList.list.map {
        broker =>
          val brokerIdentity = BrokerIdentity.from(broker)
          val brokerMetrics = getBrokerMetrics(brokerIdentity)
          (brokerIdentity.id, brokerMetrics)
      }.toMap
      brokerTopicPartitions = topicPartitionByBroker.map {
        case (brokerId, topicPartitions) =>
          val topicPartitionsMap : Map[TopicIdentity, IndexedSeq[Int]] = topicPartitions.map {
            case (topic, id, partitions) =>
              (topic, partitions)
          }.toMap
          (brokerId, BVView(topicPartitionsMap, brokerMetrics.get(brokerId)))
      }
    }
  }

  private [this] def getBrokerMetrics(brokerIdentity: BrokerIdentity, topic: Option[String] = None) = {
    if (brokerIdentity.jmxPort != -1) {
      KafkaJMX.connect(brokerIdentity.host, brokerIdentity.jmxPort).map {
        mbsc =>
          BrokerMetrics(
            KafkaMetrics.getBytesInPerSec(mbsc, topic),
            KafkaMetrics.getBytesOutPerSec(mbsc, topic),
            KafkaMetrics.getBytesRejectedPerSec(mbsc, topic),
            KafkaMetrics.getFailedFetchRequestsPerSec(mbsc, topic),
            KafkaMetrics.getFailedProduceRequestsPerSec(mbsc, topic),
            KafkaMetrics.getMessagesInPerSec(mbsc, topic))
      }.getOrElse {
        // Unable to connect to JMX server
        BrokerMetrics(
          RateMetric(0, 0, 0, 0, 0),
          RateMetric(0, 0, 0, 0, 0),
          RateMetric(0, 0, 0, 0, 0),
          RateMetric(0, 0, 0, 0, 0),
          RateMetric(0, 0, 0, 0, 0),
          RateMetric(0, 0, 0, 0, 0))
      }
    } else {
      // JMX Port is not configured
      BrokerMetrics(
        RateMetric(0, 0, 0, 0, 0),
        RateMetric(0, 0, 0, 0, 0),
        RateMetric(0, 0, 0, 0, 0),
        RateMetric(0, 0, 0, 0, 0),
        RateMetric(0, 0, 0, 0, 0),
        RateMetric(0, 0, 0, 0, 0))
    }
  }
}
