/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import akka.actor.{ActorRef, Cancellable, ActorPath}
import kafka.manager.features.{KMDisplaySizeFeature, KMJMXMetricsFeature}
import kafka.manager.utils.FiniteQueue
import org.joda.time.DateTime

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Try

/**
 * @author hiral
 */
import ActorModel._
case class BrokerViewCacheActorConfig(kafkaStateActorPath: ActorPath,
                                      clusterContext: ClusterContext,
                                      longRunningPoolConfig: LongRunningPoolConfig,
                                      updatePeriod: FiniteDuration = 10 seconds)
class BrokerViewCacheActor(config: BrokerViewCacheActorConfig) extends LongRunningPoolActor {

  private[this] val ZERO = BigDecimal(0)

  private[this] var cancellable : Option[Cancellable] = None

  private[this] var topicIdentities : Map[String, TopicIdentity] = Map.empty

  private[this] var previousTopicDescriptionsOption : Option[TopicDescriptions] = None
  
  private[this] var topicDescriptionsOption : Option[TopicDescriptions] = None

  private[this] var topicConsumerMap : Map[String, Iterable[String]] = Map.empty

  private[this] var consumerIdentities : Map[String, ConsumerIdentity] = Map.empty

  private[this] var consumerDescriptionsOption : Option[ConsumerDescriptions] = None

  private[this] var brokerListOption : Option[BrokerList] = None

  private[this] var brokerMetrics : Map[Int, BrokerMetrics] = Map.empty

  private[this] val brokerTopicPartitions : mutable.Map[Int, BVView] = new mutable.HashMap[Int, BVView]

  private[this] val topicMetrics: mutable.Map[String, mutable.Map[Int, BrokerMetrics]] =
    new mutable.HashMap[String, mutable.Map[Int, BrokerMetrics]]()

  // topic -> partitions -> brokers
  private[this] val brokerTopicPartitionSizes: mutable.Map[String, mutable.Map[Int, mutable.Map[Int, Long]]] =
    new mutable.HashMap[String, mutable.Map[Int, mutable.Map[Int, Long]]]()

  private[this] var combinedBrokerMetric : Option[BrokerMetrics] = None

  private[this] val EMPTY_BVVIEW = BVView(Map.empty, config.clusterContext, Option(BrokerMetrics.DEFAULT))

  private[this] var brokerMessagesPerSecCountHistory : Map[Int, Queue[BrokerMessagesPerSecCount]] = Map.empty

  override def preStart() = {
    log.info("Started actor %s".format(self.path))
    log.info("Scheduling updater for %s".format(config.updatePeriod))
    cancellable = Some(
      context.system.scheduler.schedule(0 seconds,
        config.updatePeriod,
        self,
        BVForceUpdate)(context.system.dispatcher,self)
    )
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

  private def produceBViewWithBrokerClusterState(bv: BVView, id: Int) : BVView = {
    val bcs = for {
      metrics <- bv.metrics
      cbm <- combinedBrokerMetric
    } yield {
        val perMessages = if(cbm.messagesInPerSec.oneMinuteRate > 0) {
          BigDecimal(metrics.messagesInPerSec.oneMinuteRate / cbm.messagesInPerSec.oneMinuteRate * 100D).setScale(3, BigDecimal.RoundingMode.HALF_UP)
        } else ZERO
        val perIncoming = if(cbm.bytesInPerSec.oneMinuteRate > 0) {
          BigDecimal(metrics.bytesInPerSec.oneMinuteRate / cbm.bytesInPerSec.oneMinuteRate * 100D).setScale(3, BigDecimal.RoundingMode.HALF_UP)
        } else ZERO
        val perOutgoing = if(cbm.bytesOutPerSec.oneMinuteRate > 0) {
          BigDecimal(metrics.bytesOutPerSec.oneMinuteRate / cbm.bytesOutPerSec.oneMinuteRate * 100D).setScale(3, BigDecimal.RoundingMode.HALF_UP)
        } else ZERO
        BrokerClusterStats(perMessages, perIncoming, perOutgoing)
      }
    val messagesPerSecCountHistory = brokerMessagesPerSecCountHistory.get(id)
    if(bcs.isDefined) {
      bv.copy(stats = bcs, messagesPerSecCountHistory = messagesPerSecCountHistory)
    } else {
      bv.copy(messagesPerSecCountHistory = messagesPerSecCountHistory)
    }
  }

  private def allBrokerViews(): Map[Int, BVView] = {
    val bvs = mutable.Map[Int, BVView]()
    for (key <- brokerTopicPartitions.keySet.toSeq.sorted) {
      val bv = brokerTopicPartitions.get(key).map { bv => produceBViewWithBrokerClusterState(bv, key) }
      if (bv.isDefined) {
        bvs.put(key, bv.get)
      }
    }
    bvs.toMap
  }

  override def processActorRequest(request: ActorRequest): Unit = {
    request match {
      case BVForceUpdate =>
        log.info("Updating broker view...")
        // ask for topic descriptions
        val lastUpdateMillisOption: Option[Long] = topicDescriptionsOption.map(_.lastUpdateMillis)
        // upon receiving topic descriptions, it will ask for broker list
        context.actorSelection(config.kafkaStateActorPath).tell(KSGetAllTopicDescriptions(lastUpdateMillisOption), self)
        if (config.clusterContext.config.pollConsumers) {
          context.actorSelection(config.kafkaStateActorPath).tell(KSGetAllConsumerDescriptions(lastUpdateMillisOption), self)
        }

      case BVGetViews =>
        sender ! allBrokerViews()


      case BVGetView(id) =>
        sender ! brokerTopicPartitions.get(id).map { bv =>
          produceBViewWithBrokerClusterState(bv, id)
        }

      case BVGetBrokerMetrics =>
        sender ! brokerMetrics

      case BVGetTopicMetrics(topic) =>
        sender ! topicMetrics.get(topic).map(m => m.values.foldLeft(BrokerMetrics.DEFAULT)((acc,bm) => acc + bm))

      case BVGetTopicIdentities =>
        sender ! topicIdentities

      case BVGetTopicConsumerMap =>
        sender ! topicConsumerMap

      case BVGetConsumerIdentities =>
        sender ! consumerIdentities

      case BVGetBrokerTopicPartitionSizes(topic) =>
        sender ! brokerTopicPartitionSizes.get(topic).map(m => m.map{case (k,v) => (k, v.toMap)}.toMap)

      case BVUpdateTopicMetricsForBroker(id, metrics) =>
        metrics.foreach {
          case (topic, bm) =>
            val tm = topicMetrics.getOrElse(topic, new mutable.HashMap[Int, BrokerMetrics])
            tm.put(id, bm)
            topicMetrics.put(topic, tm)
        }

      case BVUpdateBrokerMetrics(id, metrics) =>
        brokerMetrics += (id -> metrics)
        combinedBrokerMetric = Option(brokerMetrics.values.foldLeft(BrokerMetrics.DEFAULT)((acc, m) => acc + m))

        val updatedBVView = brokerTopicPartitions.getOrElse(id, EMPTY_BVVIEW).copy(metrics = Option(metrics))
        brokerTopicPartitions.put(id, updatedBVView)
        val now = DateTime.now()
        val messagesCount = BrokerMessagesPerSecCount(now, metrics.messagesInPerSec.count)
        brokerMessagesPerSecCountHistory += (id -> brokerMessagesPerSecCountHistory.get(id).map {
          history =>
            history.enqueueFinite(messagesCount, 10)
        }.getOrElse {
          Queue(messagesCount)
        })

      case BVUpdateBrokerTopicPartitionSizes(id, logInfo) =>
        for ((topic, partitions) <- logInfo) {
          val tMap = brokerTopicPartitionSizes.getOrElse(topic, new mutable.HashMap[Int, mutable.Map[Int, Long]])
          for ((partition, info) <- partitions; pMap = tMap.getOrElse(partition, new mutable.HashMap[Int, Long])) {
            pMap.put(id, info.bytes)
            tMap.put(partition, pMap)
          }
          brokerTopicPartitionSizes.put(topic, tMap)
        }

      case any: Any => log.warning("bvca : processActorRequest : Received unknown message: {}", any)
    }
  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case td: TopicDescriptions =>
        previousTopicDescriptionsOption = topicDescriptionsOption
        topicDescriptionsOption = Some(td)
        context.actorSelection(config.kafkaStateActorPath).tell(KSGetBrokers, self)

      case cd: ConsumerDescriptions =>
        consumerDescriptionsOption = Some(cd)
        updateViewsForConsumers()

      case bl: BrokerList =>
        brokerListOption = Some(bl)
        updateViewForBrokersAndTopics()

      case any: Any => log.warning("bvca : processActorResponse : Received unknown message: {}", any)
    }
  }

  implicit def queue2finitequeue[A](q: Queue[A]): FiniteQueue[A] = new FiniteQueue[A](q)

  private[this] def updateViewForBrokersAndTopics(): Unit = {
    for {
      brokerList <- brokerListOption
      topicDescriptions <- topicDescriptionsOption
      previousDescriptionsMap: Option[Map[String, TopicDescription]] = previousTopicDescriptionsOption.map(_.descriptions.map(td => (td.topic, td)).toMap)
    } {
      val topicIdentity : IndexedSeq[TopicIdentity] = topicDescriptions.descriptions.map {
        tdCurrent =>
          val tpm = brokerTopicPartitionSizes.get(tdCurrent.topic).map(m => m.map{case (k,v) => (k, v.toMap)}.toMap)
          TopicIdentity.from(brokerList.list.size, tdCurrent, None, tpm, config.clusterContext, previousDescriptionsMap.flatMap(_.get(tdCurrent.topic)))
        
      }
      topicIdentities = topicIdentity.map(ti => (ti.topic, ti)).toMap
      val topicPartitionByBroker = topicIdentity.flatMap(
        ti => ti.partitionsByBroker.map(btp => (ti,btp.id,btp.partitions))).groupBy(_._2)

      //check for 3*broker list size since we schedule 3 jmx calls for each broker
      if (config.clusterContext.clusterFeatures.features(KMJMXMetricsFeature) && config.clusterContext.clusterFeatures.features(KMDisplaySizeFeature) && hasCapacityFor(3*brokerListOption.size)) {
        implicit val ec = longRunningExecutionContext
        updateTopicMetrics(brokerList, topicPartitionByBroker, shouldGetBrokerSize = true)
        updateBrokerMetrics(brokerList, shouldGetBrokerSize = true)
        updateBrokerTopicPartitionsSize(brokerList)
      } else if (config.clusterContext.clusterFeatures.features(KMJMXMetricsFeature) && hasCapacityFor(2*brokerListOption.size)) {
        implicit val ec = longRunningExecutionContext
        updateTopicMetrics(brokerList, topicPartitionByBroker)
        updateBrokerMetrics(brokerList)
      } else if(config.clusterContext.clusterFeatures.features(KMJMXMetricsFeature)) {
        log.warning("Not scheduling update of JMX for all brokers, not enough capacity!")
      }

      topicPartitionByBroker.foreach {
        case (brokerId, topicPartitions) =>
          val topicPartitionsMap: Map[TopicIdentity, IndexedSeq[Int]] = topicPartitions.map {
            case (topic, id, partitions) =>
              (topic, partitions)
          }.toMap
          brokerTopicPartitions.put(
            brokerId, BVView(topicPartitionsMap, config.clusterContext, brokerMetrics.get(brokerId)))
      }
    }
  }

  private[this] def updateViewsForConsumers(): Unit = {
    for {
      consumerDescriptions <- consumerDescriptionsOption
    } {
      val consumerIdentity : IndexedSeq[ConsumerIdentity] = consumerDescriptions.descriptions.map(
          ConsumerIdentity.from(_, config.clusterContext))
      consumerIdentities = consumerIdentity.map(ci => (ci.consumerGroup, ci)).toMap

      val c2tMap = consumerDescriptions.descriptions.map{cd: ConsumerDescription =>
        (cd.consumer, cd.topics.keys.toList)}.toMap
      topicConsumerMap = c2tMap.values.flatten.map(v => (v, c2tMap.keys.filter(c2tMap(_).contains(v)))).toMap
    }
  }

  private def updateTopicMetrics(brokerList: BrokerList,
    topicPartitionByBroker: Map[Int, IndexedSeq[(TopicIdentity, Int, IndexedSeq[Int])]],
    shouldGetBrokerSize: Boolean = false
    )(implicit ec: ExecutionContext): Unit = {
    val brokerLookup = brokerList.list.map(bi => bi.id -> bi).toMap
    topicPartitionByBroker.foreach {
      case (brokerId, topicPartitions) =>
        val brokerInfoOpt = brokerLookup.get(brokerId)
        brokerInfoOpt.foreach {
          broker =>
            longRunning {
              Future {
                val tryResult = KafkaJMX.doWithConnection(broker.host, broker.jmxPort,
                  config.clusterContext.config.jmxUser, config.clusterContext.config.jmxPass
                ) {
                  mbsc =>
                    topicPartitions.map {
                      case (topic, id, partitions) =>
                        (topic.topic,
                          KafkaMetrics.getBrokerMetrics(config.clusterContext.config.version, mbsc, shouldGetBrokerSize, Option(topic.topic)))
                    }
                }
                val result = tryResult match {
                  case scala.util.Failure(t) =>
                    log.error(t, s"Failed to get topic metrics for broker $broker")
                    topicPartitions.map {
                      case (topic, id, partitions) =>
                        (topic.topic, BrokerMetrics.DEFAULT)
                    }
                  case scala.util.Success(bm) => bm
                }
                self.tell(BVUpdateTopicMetricsForBroker(broker.id, result), ActorRef.noSender)
              }
            }
        }
    }
  }

  private def updateBrokerMetrics(brokerList: BrokerList, shouldGetBrokerSize: Boolean = false)(implicit ec: ExecutionContext): Unit = {
    brokerList.list.foreach {
      broker =>
        longRunning {
          Future {
            val tryResult = KafkaJMX.doWithConnection(broker.host, broker.jmxPort,
              config.clusterContext.config.jmxUser, config.clusterContext.config.jmxPass
            ) {
              mbsc =>
                KafkaMetrics.getBrokerMetrics(config.clusterContext.config.version, mbsc, shouldGetBrokerSize)
            }

            val result = tryResult match {
              case scala.util.Failure(t) =>
                log.error(t, s"Failed to get broker metrics for $broker")
                BrokerMetrics.DEFAULT
              case scala.util.Success(bm) => bm
            }
            self.tell(BVUpdateBrokerMetrics(broker.id, result), ActorRef.noSender)
          }
        }
    }
  }

  private def updateBrokerTopicPartitionsSize(brokerList: BrokerList)(implicit ec: ExecutionContext): Unit = {
    brokerList.list.foreach {
      broker =>
        longRunning {
          Future {
            val tryResult = KafkaJMX.doWithConnection(broker.host, broker.jmxPort,
              config.clusterContext.config.jmxUser, config.clusterContext.config.jmxPass
            ) {
              mbsc =>
                KafkaMetrics.getLogSegmentsInfo(mbsc)
            }

            val result = tryResult match {
              case scala.util.Failure(t) =>
                log.error(t, s"Failed to get broker topic segment metrics for $broker")
                Map[String, Map[Int, LogInfo]]()
              case scala.util.Success(segmentInfo) => segmentInfo
            }
            self.tell(BVUpdateBrokerTopicPartitionSizes(broker.id, result), ActorRef.noSender)
          }
        }
    }
  }
}
