/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import akka.actor.{ActorRef, Cancellable, ActorPath}
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

/**
 * @author hiral
 */
import ActorModel._
case class BrokerViewCacheActorConfig(kafkaStateActorPath: ActorPath, 
                                      clusterConfig: ClusterConfig, 
                                      longRunningPoolConfig: LongRunningPoolConfig, 
                                      updatePeriod: FiniteDuration = 10 seconds)
class BrokerViewCacheActor(config: BrokerViewCacheActorConfig) extends LongRunningPoolActor {
  
  private[this] val ZERO = BigDecimal(0)

  private[this] var cancellable : Option[Cancellable] = None

  private[this] var topicIdentities : Map[String, TopicIdentity] = Map.empty

  private[this] var topicDescriptionsOption : Option[TopicDescriptions] = None

  private[this] var brokerListOption : Option[BrokerList] = None

  private[this] var brokerMetrics : Map[Int, BrokerMetrics] = Map.empty
  
  private[this] val brokerTopicPartitions : mutable.Map[Int, BVView] = new mutable.HashMap[Int, BVView]

  private[this] val topicMetrics: mutable.Map[String, mutable.Map[Int, BrokerMetrics]] =
    new mutable.HashMap[String, mutable.Map[Int, BrokerMetrics]]()
  
  private[this] var combinedBrokerMetric : Option[BrokerMetrics] = None
  
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

  override def processActorRequest(request: ActorRequest): Unit = {
    request match {
      case BVForceUpdate =>
        log.info("Updating broker view...")
        //ask for topic descriptions
        val lastUpdateMillisOption: Option[Long] = topicDescriptionsOption.map(_.lastUpdateMillis)
        context.actorSelection(config.kafkaStateActorPath).tell(KSGetAllTopicDescriptions(lastUpdateMillisOption), self)
        context.actorSelection(config.kafkaStateActorPath).tell(KSGetBrokers, self)

      case BVGetView(id) =>
        sender ! brokerTopicPartitions.get(id).map { bv =>
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
          if(bcs.isDefined) {
            bv.copy(stats=bcs)
          } else {
            bv
          }
        }
        
      case BVGetBrokerMetrics =>
        sender ! brokerMetrics

      case BVGetTopicMetrics(topic) =>
        sender ! topicMetrics.get(topic).map(m => m.values.foldLeft(BrokerMetrics.DEFAULT)((acc,bm) => acc + bm))

      case BVGetTopicIdentities =>
        sender ! topicIdentities

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
        for {
          bv <- brokerTopicPartitions.get(id)
        } {
          brokerTopicPartitions.put(id, bv.copy(metrics = Option(metrics)))
        }

      case any: Any => log.warning("bvca : processActorRequest : Received unknown message: {}", any)
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

      case any: Any => log.warning("bvca : processActorResponse : Received unknown message: {}", any)
    }
  }

  private[this] def updateView(): Unit = {
    for {
      brokerList <- brokerListOption
      topicDescriptions <- topicDescriptionsOption
    } {
      val topicIdentity : IndexedSeq[TopicIdentity] = topicDescriptions.descriptions.map(
        TopicIdentity.from(brokerList.list.size,_,None, config.clusterConfig))
      topicIdentities = topicIdentity.map(ti => (ti.topic, ti)).toMap
      val topicPartitionByBroker = topicIdentity.flatMap(
        ti => ti.partitionsByBroker.map(btp => (ti,btp.id,btp.partitions))).groupBy(_._2)

      //check for 2*broker list size since we schedule 2 jmx calls for each broker
      if (config.clusterConfig.jmxEnabled && hasCapacityFor(2*brokerListOption.size)) {
        implicit val ec = longRunningExecutionContext
        val brokerLookup = brokerList.list.map(bi => bi.id -> bi).toMap
        topicPartitionByBroker.foreach {
          case (brokerId, topicPartitions) =>
            val brokerInfoOpt = brokerLookup.get(brokerId)
            brokerInfoOpt.foreach {
              broker =>
                longRunning {
                  Future {
                    val tryResult = KafkaJMX.doWithConnection(broker.host, broker.jmxPort) {
                      mbsc =>
                        topicPartitions.map {
                          case (topic, id, partitions) =>
                            (topic.topic, 
                              KafkaMetrics.getBrokerMetrics(config.clusterConfig.version, mbsc, Option(topic.topic)))
                        }
                    }
                    val result = tryResult match {
                      case scala.util.Failure(t) =>
                        log.error(s"Failed to get topic metrics for broker $broker", t)
                        topicPartitions.map {
                          case (topic, id, partitions) =>
                            (topic.topic, BrokerMetrics.DEFAULT)
                        }
                      case scala.util.Success(bm) => bm
                    }
                    self.tell(BVUpdateTopicMetricsForBroker(broker.id,result), ActorRef.noSender)
                  }
                }
            }
        }

        brokerList.list.foreach {
          broker =>
            longRunning {
              Future {
                val tryResult = KafkaJMX.doWithConnection(broker.host, broker.jmxPort) {
                  mbsc =>
                    KafkaMetrics.getBrokerMetrics(config.clusterConfig.version, mbsc)
                }

                val result = tryResult match {
                  case scala.util.Failure(t) =>
                    log.error(s"Failed to get broker metrics for $broker", t)
                    BrokerMetrics.DEFAULT
                  case scala.util.Success(bm) => bm
                }
                self.tell(BVUpdateBrokerMetrics(broker.id,result), ActorRef.noSender)
              }
            }
        }
      } else if(config.clusterConfig.jmxEnabled) {
        log.warning("Not scheduling update of JMX for all brokers, not enough capacity!")
      }
      
      topicPartitionByBroker.foreach {
        case (brokerId, topicPartitions) =>
          val topicPartitionsMap : Map[TopicIdentity, IndexedSeq[Int]] = topicPartitions.map {
            case (topic, id, partitions) =>
              (topic, partitions)
          }.toMap
          brokerTopicPartitions.put(
            brokerId,BVView(topicPartitionsMap, config.clusterConfig, brokerMetrics.get(brokerId)))
      }
    }
  }
}
