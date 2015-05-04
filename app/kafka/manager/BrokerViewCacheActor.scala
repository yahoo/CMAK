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
class BrokerViewCacheActor(kafkaStateActorPath: ActorPath, clusterConfig: ClusterConfig, updatePeriod: FiniteDuration = 10 seconds) extends BaseActor {

  private[this] var cancellable : Option[Cancellable] = None

  private[this] var brokerTopicPartitions : Map[Int, BVView] = Map.empty

  private[this] var topicIdentities : IndexedSeq[TopicIdentity] = IndexedSeq.empty

  private[this] var topicDescriptionsOption : Option[TopicDescriptions] = None

  private[this] var brokerListOption : Option[BrokerList] = None

  private[this] var topicMetrics: Map[String, Map[Int, BrokerMetrics]] = Map.empty

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

      case BVGetTopicMetrics(topic) =>
        sender ! topicMetrics.get(topic).map(m => m.values.foldLeft(BrokerMetrics.DEFAULT)((acc,bm) => acc + bm))

      case BVGetTopicIdentities =>
        sender ! topicIdentities

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
      topicIdentities = topicIdentity
      val topicPartitionByBroker = topicIdentity.flatMap(ti => ti.partitionsByBroker.map(btp => (ti,btp.id,btp.partitions))).groupBy(_._2)

      if (clusterConfig.jmxEnabled) {
        val brokerLookup = brokerList.list.map(bi => bi.id -> bi).toMap
        val topicMetricsByBroker = topicPartitionByBroker.flatMap {
          case (brokerId, topicPartitions) =>
            val brokerInfoOpt = brokerLookup.get(brokerId)
            brokerInfoOpt.map {
              broker =>
                val tryResult = KafkaJMX.doWithConnection(broker.host, broker.jmxPort) {
                  mbsc =>
                    topicPartitions.map {
                      case (topic, id, partitions) =>
                        ((brokerId, topic.topic), KafkaMetrics.getBrokerMetrics(mbsc, Option(topic.topic)))
                    }
                }
                tryResult match {
                  case scala.util.Failure(t) =>
                    log.error(s"Failed to get topic metrics for broker $broker",t)
                    topicPartitions.map {
                      case (topic, id, partitions) =>
                        ((brokerId, topic.topic), BrokerMetrics.DEFAULT)
                    }
                  case scala.util.Success(bm) => bm
                }
            }
        }.flatten
        val groupedByTopic = topicMetricsByBroker.groupBy(_._1._2)
        topicMetrics = groupedByTopic.mapValues { metricList =>
            metricList.map(m => m._1._1 -> m._2).toMap
        }

        brokerMetrics = brokerList.list.map {
          broker =>
            val tryResult = KafkaJMX.doWithConnection(broker.host, broker.jmxPort) {
              mbsc =>
                val brokerMetrics = KafkaMetrics.getBrokerMetrics(mbsc)
                (broker.id, brokerMetrics)
            }

            tryResult match {
              case scala.util.Failure(t) =>
                log.error(s"Failed to get broker metrics for $broker",t)
                broker.id -> BrokerMetrics.DEFAULT
              case scala.util.Success(bm) => bm
            }

        }.toMap
      }
      
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
}
