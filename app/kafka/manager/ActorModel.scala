/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import java.util.Properties

import org.joda.time.DateTime
import kafka.common.TopicAndPartition
import org.slf4j.LoggerFactory

import scala.collection.immutable.Queue
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.{Try, Success, Failure}
import scalaz.{NonEmptyList, Validation}

/**
 * @author hiral
 */
object ActorModel {
  trait ActorRequest
  trait ActorResponse

  trait CommandRequest extends ActorRequest
  trait CommandResponse extends ActorResponse

  trait QueryRequest extends ActorRequest
  trait QueryResponse extends ActorResponse

  case class ActorErrorResponse(msg: String, throwableOption: Option[Throwable] = None) extends ActorResponse

  sealed trait BVRequest extends QueryRequest

  case object BVForceUpdate extends CommandRequest
  case object BVGetTopicIdentities extends BVRequest
  case object BVGetTopicConsumerMap extends BVRequest
  case object BVGetConsumerIdentities extends BVRequest
  case class BVGetView(id: Int) extends BVRequest
  case object BVGetViews extends BVRequest
  case class BVGetTopicMetrics(topic: String) extends BVRequest
  case object BVGetBrokerMetrics extends BVRequest
  case class BVView(topicPartitions: Map[TopicIdentity, IndexedSeq[Int]], clusterContext: ClusterContext,
                    metrics: Option[BrokerMetrics] = None,
                    messagesPerSecCountHistory: Option[Queue[BrokerMessagesPerSecCount]] = None,
                    stats: Option[BrokerClusterStats] = None) extends QueryResponse {
    def numTopics : Int = topicPartitions.size
    def numPartitions : Int = topicPartitions.values.foldLeft(0)((acc,i) => acc + i.size)
  }
  case class BVUpdateTopicMetricsForBroker(id: Int, metrics: IndexedSeq[(String,BrokerMetrics)]) extends CommandRequest
  case class BVUpdateBrokerMetrics(id: Int, metric: BrokerMetrics) extends CommandRequest

  case object CMGetView extends QueryRequest
  case class CMGetTopicIdentity(topic: String) extends QueryRequest
  case object CMGetClusterContext extends QueryRequest
  case class CMView(topicsCount: Int, brokersCount: Int, clusterContext: ClusterContext) extends QueryResponse
  case class CMGetConsumerIdentity(consumer: String) extends QueryRequest
  case class CMGetConsumedTopicState(consumer: String, topic: String) extends QueryRequest
  case class CMTopicIdentity(topicIdentity: Try[TopicIdentity]) extends QueryResponse
  case class CMConsumerIdentity(consumerIdentity: Try[ConsumerIdentity]) extends QueryResponse
  case class CMConsumedTopic(ctIdentity: Try[ConsumedTopicState]) extends QueryResponse
  case object CMShutdown extends CommandRequest
  case class CMCreateTopic(topic: String,
                           partitions: Int,
                           replicationFactor: Int,
                           config: Properties = new Properties) extends CommandRequest
  case class CMAddTopicPartitions(topic: String,
                                  brokers: Seq[Int],
                                  partitions: Int,
                                  partitionReplicaList: Map[Int, Seq[Int]],
                                  readVersion: Int) extends CommandRequest
  case class CMAddMultipleTopicsPartitions(topicsAndReplicas: Seq[(String, Map[Int, Seq[Int]])],
                                           brokers: Seq[Int],
                                           partitions: Int,
                                           readVersions: Map[String,Int]) extends CommandRequest
  case class CMUpdateTopicConfig(topic: String, config: Properties, readVersion: Int) extends CommandRequest
  case class CMDeleteTopic(topic: String) extends CommandRequest
  case class CMRunPreferredLeaderElection(topics: Set[String]) extends CommandRequest
  case class CMRunReassignPartition(topics: Set[String]) extends CommandRequest
  case class CMGeneratePartitionAssignments(topics: Set[String], brokers: Seq[Int]) extends CommandRequest
  case class CMManualPartitionAssignments(assignments: List[(String, List[(Int, List[Int])])]) extends CommandRequest

  //these are used by Logkafka
  //##########
  case class CMGetLogkafkaIdentity(hostname: String) extends QueryRequest
  case class CMLogkafkaIdentity(logkafkaIdentity: Try[LogkafkaIdentity]) extends QueryResponse
  case class CMCreateLogkafka(hostname: String,
                              log_path: String,
                              config: Properties = new Properties
                              ) extends CommandRequest
  case class CMUpdateLogkafkaConfig(hostname: String, 
                                    log_path: String, 
                                    config: Properties) extends CommandRequest
  case class CMDeleteLogkafka(hostname: String, log_path: String) extends CommandRequest
  //##########

  case class CMCommandResult(result: Try[ClusterContext]) extends CommandResponse
  case class CMCommandResults(result: IndexedSeq[Try[Unit]]) extends CommandResponse

  case class KCCreateTopic(topic: String,
                           brokers: Seq[Int],
                           partitions: Int,
                           replicationFactor:Int,
                           config: Properties) extends CommandRequest
  case class KCAddTopicPartitions(topic: String,
                           brokers: Seq[Int],
                           partitions: Int,
                           partitionReplicaList: Map[Int, Seq[Int]],
                           readVersion: Int) extends CommandRequest
  case class KCAddMultipleTopicsPartitions(topicsAndReplicas: Seq[(String, Map[Int, Seq[Int]])],
                                           brokers: Seq[Int],
                                           partitions: Int,
                                           readVersions: Map[String, Int]) extends CommandRequest
  case class KCUpdateTopicConfig(topic: String, config: Properties, readVersion: Int) extends CommandRequest
  case class KCDeleteTopic(topic: String) extends CommandRequest
  case class KCPreferredReplicaLeaderElection(topicAndPartition: Set[TopicAndPartition]) extends CommandRequest
  case class KCReassignPartition(currentTopicIdentity: Map[String, TopicIdentity],
                               generatedTopicIdentity: Map[String, TopicIdentity]) extends CommandRequest

  case class KCCommandResult(result: Try[Unit]) extends CommandResponse

  case object KMGetActiveClusters extends QueryRequest
  case object KMGetAllClusters extends QueryRequest
  case class KMGetClusterConfig(clusterName: String) extends QueryRequest
  case class KMClusterQueryRequest(clusterName: String, request: QueryRequest) extends QueryRequest
  case class KMQueryResult(result: IndexedSeq[ClusterConfig]) extends QueryResponse
  case class KMClusterConfigResult(result: Try[ClusterConfig]) extends QueryResponse
  case class KMClusterList(active: IndexedSeq[ClusterConfig], pending : IndexedSeq[ClusterConfig]) extends QueryResponse

  case object KMUpdateState extends CommandRequest
  case object KMPruneClusters extends CommandRequest
  case object KMShutdown extends CommandRequest
  case object KMShutdownComplete extends CommandResponse
  case class KMAddCluster(config: ClusterConfig) extends CommandRequest
  case class KMUpdateCluster(config: ClusterConfig) extends CommandRequest
  case class KMEnableCluster(clusterName: String) extends CommandRequest
  case class KMDisableCluster(clusterName: String) extends CommandRequest
  case class KMDeleteCluster(clusterName: String) extends CommandRequest
  case class KMClusterCommandRequest(clusterName: String, request: CommandRequest) extends CommandRequest
  case class KMCommandResult(result: Try[Unit]) extends CommandResponse

  sealed trait KSRequest extends QueryRequest
  case object KSGetTopics extends KSRequest
  case object KSGetConsumers extends KSRequest
  case class KSGetTopicConfig(topic: String) extends KSRequest
  case class KSGetTopicDescription(topic: String) extends KSRequest
  case class KSGetAllTopicDescriptions(lastUpdateMillis: Option[Long]= None) extends KSRequest
  case class KSGetTopicDescriptions(topics: Set[String]) extends KSRequest
  case class KSGetConsumerDescription(consumer: String) extends KSRequest
  case class KSGetConsumedTopicDescription(consumer: String, topic: String) extends KSRequest
  case class KSGetAllConsumerDescriptions(lastUpdateMillis: Option[Long]= None) extends KSRequest
  case class KSGetConsumerDescriptions(consumers: Set[String]) extends KSRequest
  case object KSGetTopicsLastUpdateMillis extends KSRequest
  case object KSGetPreferredLeaderElection extends KSRequest
  case object KSGetReassignPartition extends KSRequest
  case class KSEndPreferredLeaderElection(millis: Long) extends CommandRequest
  case class KSUpdatePreferredLeaderElection(millis: Long, json: String) extends CommandRequest
  case class KSEndReassignPartition(millis: Long) extends CommandRequest
  case class KSUpdateReassignPartition(millis: Long, json: String) extends CommandRequest

  case object KSGetBrokers extends KSRequest
  case class KSGetBrokerState(id: String) extends  KSRequest

  case class TopicList(list: IndexedSeq[String], deleteSet: Set[String], clusterContext: ClusterContext) extends QueryResponse
  case class TopicConfig(topic: String, config: Option[(Int,String)]) extends QueryResponse
  case class ConsumerList(list: IndexedSeq[String], clusterContext: ClusterContext) extends QueryResponse

  case class TopicDescription(topic: String,
                              description: (Int,String),
                              partitionState: Option[Map[String, String]], 
                              partitionOffsets: Future[PartitionOffsetsCapture],
                              config:Option[(Int,String)]) extends  QueryResponse
  case class TopicDescriptions(descriptions: IndexedSeq[TopicDescription], lastUpdateMillis: Long) extends QueryResponse

  case class BrokerList(list: IndexedSeq[BrokerIdentity], clusterContext: ClusterContext) extends QueryResponse

  case class PreferredReplicaElection(startTime: DateTime, 
                                      topicAndPartition: Set[TopicAndPartition], 
                                      endTime: Option[DateTime], 
                                      clusterContext: ClusterContext) extends QueryResponse
  case class ReassignPartitions(startTime: DateTime, 
                                partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]], 
                                endTime: Option[DateTime], 
                                clusterContext: ClusterContext) extends QueryResponse

  case class ConsumedTopicDescription(consumer: String,
                                      topic: String,
                                      numPartitions: Int,
                                      topicDescription: Option[TopicDescription],
                                      partitionOwners: Option[Map[Int, String]],
                                      partitionOffsets: Option[Map[Int, Long]])
  case class ConsumerDescription(consumer: String,
                                 topics: Map[String, ConsumedTopicDescription]) extends  QueryResponse
  case class ConsumerDescriptions(descriptions: IndexedSeq[ConsumerDescription], lastUpdateMillis: Long) extends QueryResponse

  case object DCUpdateState extends CommandRequest

  case class BrokerIdentity(id: Int, host: String, port: Int, jmxPort: Int)

  object BrokerIdentity {
    import scalaz.syntax.applicative._
    import org.json4s.jackson.JsonMethods._
    import org.json4s.scalaz.JsonScalaz
    import org.json4s.scalaz.JsonScalaz._
    import scala.language.reflectiveCalls

    implicit def from(id: Int, config: String): Validation[NonEmptyList[JsonScalaz.Error],BrokerIdentity]= {
      val json = parse(config)
      (field[String]("host")(json) |@| field[Int]("port")(json) |@| field[Int]("jmx_port")(json))
      {
        (host: String, port: Int, jmxPort: Int) => BrokerIdentity(id,host, port, jmxPort)
      }
    }
  }

  case class TopicPartitionIdentity(partNum: Int,
                                    leader: Int,
                                    latestOffset: Option[Long],
                                    rateOfChange: Option[Double],
                                    isr: Seq[Int],
                                    replicas: Seq[Int],
                                    isPreferredLeader: Boolean = false,
                                    isUnderReplicated: Boolean = false)
  object TopicPartitionIdentity {

    lazy val logger = LoggerFactory.getLogger(this.getClass)

    import scalaz.syntax.applicative._
    import org.json4s.jackson.JsonMethods._
    import org.json4s.scalaz.JsonScalaz._
    import scala.language.reflectiveCalls

    implicit def from(partition: Int,
                      state:Option[String],
                      offset: Option[Long],
                      rateOfChange: Option[Double],
                      replicas: Seq[Int]) : TopicPartitionIdentity = {
      val leaderAndIsr = for {
        json <- state
        parsedJson = parse(json)
      } yield {
        (field[Int]("leader")(parsedJson) |@| field[List[Int]]("isr")(parsedJson)) {
          (leader: Int, isr: Seq[Int]) => leader -> isr
        }
      }
      val default = TopicPartitionIdentity(partition,
                                           -2,
                                           offset,
                                           rateOfChange,
                                           Seq.empty,
                                           replicas)
      leaderAndIsr.fold(default) { parsedLeaderAndIsrOrError =>
        parsedLeaderAndIsrOrError.fold({ e =>
          logger.error(s"Failed to parse topic state $e")
          default
        }, {
          case (leader, isr) =>
            TopicPartitionIdentity(partition, leader, offset, rateOfChange, isr, replicas, leader == replicas.head, isr.size != replicas.size)
        })
      }
    }
  }

  case class BrokerTopicPartitions(id: Int, partitions: IndexedSeq[Int], isSkewed: Boolean)
  
  case class PartitionOffsetsCapture(updateTimeMillis: Long, offsetsMap: Map[Int, Long])

  object PartitionOffsetsCapture {
    val ZERO : Option[Double] = Option(0D)
    
    val EMPTY : PartitionOffsetsCapture = PartitionOffsetsCapture(0, Map.empty)

    def getRate(part: Int, currentOffsets: PartitionOffsetsCapture, previousOffsets: PartitionOffsetsCapture): Option[Double] = {
      val timeDiffMillis = currentOffsets.updateTimeMillis - previousOffsets.updateTimeMillis
      val offsetDif = for {
        currentOffset <- currentOffsets.offsetsMap.get(part)
        previousOffset <- previousOffsets.offsetsMap.get(part)
      } yield {
        currentOffset - previousOffset
      }
      if(timeDiffMillis > 0) {
        //multiply by 1000 since we have millis
        offsetDif.map( od => od * 1000 * 1D / timeDiffMillis)
      } else {
        PartitionOffsetsCapture.ZERO
      }
    }
  }

  case class TopicIdentity(topic:String,
                           readVersion: Int,
                           partitions:Int,
                           partitionsIdentity: Map[Int,TopicPartitionIdentity],
                           numBrokers: Int,
                           configReadVersion: Int,
                           config: List[(String,String)],
                           clusterContext: ClusterContext,
                           metrics: Option[BrokerMetrics] = None) {

    val replicationFactor : Int = partitionsIdentity.head._2.replicas.size

    val partitionsByBroker : IndexedSeq[BrokerTopicPartitions] = {
      val brokerPartitionsMap : Map[Int, Iterable[Int]] =
        partitionsIdentity.toList.flatMap(t => t._2.isr.map(i => (i,t._2.partNum))).groupBy(_._1).mapValues(_.map(_._2))

      val brokersForTopic = brokerPartitionsMap.keySet.size
      val avgPartitionsPerBroker : Double = Math.ceil((1.0 * partitions) / brokersForTopic * replicationFactor)

      brokerPartitionsMap.map {
        case (brokerId, brokerPartitions)=>
          BrokerTopicPartitions(brokerId, brokerPartitions.toIndexedSeq.sorted,
            brokerPartitions.size > avgPartitionsPerBroker)
      }.toIndexedSeq.sortBy(_.id)
    }

    // a topic's log-size is the sum of its partitions' log-sizes, we take the sum of the ones we know the offset for.
    val summedTopicOffsets : Long = partitionsIdentity.map(_._2.latestOffset).collect{case Some(offset) => offset}.sum

    val preferredReplicasPercentage : Int = (100 * partitionsIdentity.count(_._2.isPreferredLeader)) / partitions

    val underReplicatedPercentage : Int = (100 * partitionsIdentity.count(_._2.isUnderReplicated)) / partitions

    val topicBrokers : Int = partitionsByBroker.size

    val brokersSkewPercentage : Int =  {
      if(topicBrokers > 0)
        (100 * partitionsByBroker.count(_.isSkewed)) / topicBrokers
      else 0
    }

    val brokersSpreadPercentage : Int = if(numBrokers > 0) {
      (100 * topicBrokers) / numBrokers
    } else {
      100 // everthing is spreaded if nothing has to be spreaded
    }

    val producerRate: String = BigDecimal(partitionsIdentity.map(_._2.rateOfChange.getOrElse(0D)).sum).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString()
  }

  object TopicIdentity {

    lazy val logger = LoggerFactory.getLogger(this.getClass)

    import org.json4s.jackson.JsonMethods._
    import org.json4s.scalaz.JsonScalaz._
    import scala.language.reflectiveCalls
    
    private[this] def getPartitionReplicaMap(td: TopicDescription) : Map[String, List[Int]] = {
      // Get the topic description information
      val descJson = parse(td.description._2)
      field[Map[String,List[Int]]]("partitions")(descJson).fold({ e =>
        logger.error(s"[topic=${td.topic}] Failed to get partitions from topic json ${td.description._2}")
        Map.empty
      }, identity)
    }

    private[this] def getTopicPartitionIdentity(td: TopicDescription,
                                                partMap: Map[String, List[Int]],
                                                tdPrevious: Option[TopicDescription]) : Map[Int, TopicPartitionIdentity] = {

      val stateMap = td.partitionState.getOrElse(Map.empty)
      // Assign the partition data to the TPI format
      partMap.map { case (partition, replicas) =>
        val partitionNum = partition.toInt
        // block on the futures that hold the latest produced offset in each partition
        val partitionOffsets: Option[PartitionOffsetsCapture] = Await.ready(td.partitionOffsets, Duration.Inf).value.get match {
          case Success(offsetMap) =>
            Option(offsetMap)
          case Failure(e) =>
            None
        }

        val previousPartitionOffsets: Option[PartitionOffsetsCapture] = tdPrevious.flatMap {
          ptd => Await.ready(ptd.partitionOffsets, Duration.Inf).value.get match {
            case Success(offsetMap) =>
              Option(offsetMap)
            case Failure(e) =>
              None
          }
        }
        
        val currentOffsetOption = partitionOffsets.flatMap(_.offsetsMap.get(partitionNum))
        val rateOfChange = for {
          currentOffsets <- partitionOffsets
          previousOffsets <- previousPartitionOffsets
          result <- PartitionOffsetsCapture.getRate(partitionNum, currentOffsets, previousOffsets)
        } yield result

        (partitionNum,TopicPartitionIdentity.from(partitionNum,
          stateMap.get(partition),
          currentOffsetOption,
          rateOfChange,
          replicas))
      }
    }
    
    def getTopicPartitionIdentity(td: TopicDescription, tdPrevious: Option[TopicDescription]) : Map[Int, TopicPartitionIdentity] = {
      // Get the topic description information
      val partMap = getPartitionReplicaMap(td)

      getTopicPartitionIdentity(td, partMap, tdPrevious)
    }
    
    implicit def from(brokers: Int,
                      td: TopicDescription,
                      tm: Option[BrokerMetrics],
                      clusterContext: ClusterContext, tdPrevious: Option[TopicDescription]) : TopicIdentity = {
      // Get the topic description information
      val partMap = getPartitionReplicaMap(td)
      val tpi : Map[Int,TopicPartitionIdentity] = getTopicPartitionIdentity(td, partMap, tdPrevious)
      val config : (Int,Map[String, String]) = {
        try {
          val resultOption: Option[(Int,Map[String, String])] = td.config.map { configString =>
            val configJson = parse(configString._2)
            val configMap : Map[String, String] = field[Map[String,String]]("config")(configJson).fold({ e =>
              logger.error(s"Failed to parse topic config ${configString._2}")
              Map.empty
            }, identity)
            (configString._1,configMap)
          }
          resultOption.getOrElse((-1,Map.empty[String, String]))
        } catch {
          case e: Exception =>
            logger.error(s"[topic=${td.topic}] Failed to parse topic config : ${td.config.getOrElse("")}",e)
            (-1,Map.empty[String, String])
        }
      }
      TopicIdentity(td.topic,td.description._1,partMap.size,tpi,brokers,config._1,config._2.toList, clusterContext, tm)
    }

    implicit def from(bl: BrokerList,td: TopicDescription, tm: Option[BrokerMetrics], clusterContext: ClusterContext, tdPrevious: Option[TopicDescription]) : TopicIdentity = {
      from(bl.list.size, td, tm, clusterContext, tdPrevious)
    }

    implicit def reassignReplicas(currentTopicIdentity: TopicIdentity,
                                  assignedReplicas: Map[Int, Seq[Int]]) : Try[TopicIdentity] = {
      Try {
        val newTpi : Map[Int, TopicPartitionIdentity] = currentTopicIdentity.partitionsIdentity.map { case (part, tpi) =>
          val newReplicaSeq = assignedReplicas.get(part)
          require(newReplicaSeq.isDefined, s"Missing replica assignment for partition $part for topic ${currentTopicIdentity.topic}")
          val newReplicaSet = newReplicaSeq.get.toSet
          require(newReplicaSeq.get.size == newReplicaSet.size,
            s"Duplicates found in replica set ${newReplicaSeq.get} for partition $part for topic ${currentTopicIdentity.topic}")
          (part,tpi.copy(replicas = newReplicaSeq.get))
        }
        TopicIdentity(
          currentTopicIdentity.topic,
          currentTopicIdentity.readVersion,
          currentTopicIdentity.partitions,
          newTpi,
          currentTopicIdentity.numBrokers,
          currentTopicIdentity.configReadVersion,
          currentTopicIdentity.config,
          currentTopicIdentity.clusterContext,
          currentTopicIdentity.metrics)
      }
    }
  }

  case class ConsumedTopicState(consumerGroup: String,
                                topic: String,
                                numPartitions: Int,
                                partitionLatestOffsets: Map[Int, Long],
                                partitionOwners: Map[Int, String],
                                partitionOffsets: Map[Int, Long], 
                                clusterContext: ClusterContext) {
    lazy val totalLag : Option[Long] = {
      // only defined if every partition has a latest offset
      if (partitionLatestOffsets.values.size == numPartitions && partitionLatestOffsets.size == numPartitions) {
          Some(partitionLatestOffsets.values.sum - partitionOffsets.values.sum)
      } else None
    }
    def topicOffsets(partitionNum: Int) : Option[Long] = partitionLatestOffsets.get(partitionNum)

    def partitionLag(partitionNum: Int) : Option[Long] = {
      topicOffsets(partitionNum).flatMap{topicOffset =>
        partitionOffsets.get(partitionNum).map(topicOffset - _)}
    }

    // Percentage of the partitions that have an owner
    def percentageCovered : Int =
    if (numPartitions != 0) {
      val numCovered = partitionOwners.size
      100 * numCovered / numPartitions
    } else {
      100 // if there are no partitions to cover, they are all covered!
    }
  }

  object ConsumedTopicState {
    def from(ctd: ConsumedTopicDescription, clusterContext: ClusterContext): ConsumedTopicState = {
      val partitionOffsetsMap = ctd.partitionOffsets.getOrElse(Map.empty)
      val partitionOwnersMap = ctd.partitionOwners.getOrElse(Map.empty)
      // block on the futures that hold the latest produced offset in each partition
      val topicOffsetsOptMap: Map[Int, Long]= ctd.topicDescription.map{td: TopicDescription =>
        Await.ready(td.partitionOffsets, Duration.Inf).value.get match {
        case Success(offsetMap) =>
          offsetMap.offsetsMap
        case Failure(e) =>
          Map.empty[Int, Long]
      }}.getOrElse(Map.empty)

      ConsumedTopicState(
        ctd.consumer, 
        ctd.topic, 
        ctd.numPartitions, 
        topicOffsetsOptMap, 
        partitionOwnersMap, 
        partitionOffsetsMap, 
        clusterContext)
    }
  }

  case class ConsumerIdentity(consumerGroup:String,
                              topicMap: Map[String, ConsumedTopicState],
                              clusterContext: ClusterContext)
  object ConsumerIdentity {
    lazy val logger = LoggerFactory.getLogger(this.getClass)
    import scala.language.reflectiveCalls

    implicit def from(cd: ConsumerDescription,
                      clusterContext: ClusterContext) : ConsumerIdentity = {
      val topicMap: Seq[(String, ConsumedTopicState)] = for {
        (topic, ctd) <- cd.topics.toSeq
        cts = ConsumedTopicState.from(ctd, clusterContext)
      } yield (topic, cts)
      ConsumerIdentity(cd.consumer,
        topicMap.toMap,
        clusterContext)
    }

  }

  case class BrokerMessagesPerSecCount(date: DateTime,
                                       count: Long)

  case class BrokerMetrics(bytesInPerSec: MeterMetric,
                           bytesOutPerSec: MeterMetric,
                           bytesRejectedPerSec: MeterMetric,
                           failedFetchRequestsPerSec: MeterMetric,
                           failedProduceRequestsPerSec: MeterMetric,
                           messagesInPerSec: MeterMetric,
                           oSystemMetrics: OSMetric) {
    def +(o: BrokerMetrics) : BrokerMetrics = {
      BrokerMetrics(
        o.bytesInPerSec + bytesInPerSec,
        o.bytesOutPerSec + bytesOutPerSec,
        o.bytesRejectedPerSec + bytesRejectedPerSec,
        o.failedFetchRequestsPerSec + failedFetchRequestsPerSec,
        o.failedProduceRequestsPerSec + failedProduceRequestsPerSec,
        o.messagesInPerSec + messagesInPerSec,
        oSystemMetrics)
    }

  }

  object BrokerMetrics {
    val DEFAULT = BrokerMetrics(
      MeterMetric(0, 0, 0, 0, 0),
      MeterMetric(0, 0, 0, 0, 0),
      MeterMetric(0, 0, 0, 0, 0),
      MeterMetric(0, 0, 0, 0, 0),
      MeterMetric(0, 0, 0, 0, 0),
      MeterMetric(0, 0, 0, 0, 0),
      OSMetric(0D, 0D))
  }

  case class BrokerClusterStats(perMessages: BigDecimal, perIncoming: BigDecimal, perOutgoing: BigDecimal)
  
  sealed trait LKVRequest extends QueryRequest

  case object LKVForceUpdate extends CommandRequest
  case object LKVGetLogkafkaIdentities extends LKVRequest

  case class LKCCreateLogkafka(hostname: String,
                               log_path: String,
                               config: Properties,
                               logkafkaConfig: Option[LogkafkaConfig]) extends CommandRequest
  case class LKCDeleteLogkafka(hostname: String,
                               log_path: String,
                               logkafkaConfig: Option[LogkafkaConfig]) extends CommandRequest
  case class LKCUpdateLogkafkaConfig(hostname: String,
                                     log_path: String,
                                     config: Properties,
                                     logkafkaConfig: Option[LogkafkaConfig]) extends CommandRequest

  case class LKCCommandResult(result: Try[Unit]) extends CommandResponse

  sealed trait LKSRequest extends QueryRequest
  case object LKSGetLogkafkaHostnames extends LKSRequest
  case class LKSGetLogkafkaConfig(hostname: String) extends LKSRequest
  case class LKSGetLogkafkaClient(hostname: String) extends LKSRequest
  case class LKSGetLogkafkaConfigs(hostnames: Set[String]) extends LKSRequest
  case class LKSGetLogkafkaClients(hostnames: Set[String]) extends LKSRequest
  case class LKSGetAllLogkafkaConfigs(lastUpdateMillis: Option[Long]= None) extends LKSRequest
  case class LKSGetAllLogkafkaClients(lastUpdateMillis: Option[Long]= None) extends LKSRequest

  case class LogkafkaHostnameList(list: IndexedSeq[String], deleteSet: Set[String]) extends QueryResponse
  case class LogkafkaConfig(hostname: String, config: Option[String]) extends QueryResponse
  case class LogkafkaClient(hostname: String, client: Option[String]) extends QueryResponse
  case class LogkafkaConfigs(configs: IndexedSeq[LogkafkaConfig], lastUpdateMillis: Long) extends QueryResponse
  case class LogkafkaClients(clients: IndexedSeq[LogkafkaClient], lastUpdateMillis: Long) extends QueryResponse


  case class LogkafkaIdentity(hostname: String,
                              active: Boolean,
                              identityMap: Map[String, (Option[Map[String, String]], Option[Map[String, String]])]) {
  }

  object LogkafkaIdentity {

    lazy val logger = LoggerFactory.getLogger(this.getClass)

    implicit def from(hostname: String, lcg: Option[LogkafkaConfig], lct: Option[LogkafkaClient]) : LogkafkaIdentity = {
      val configJsonStr = lcg match {
        case Some(l) => l.config.getOrElse[String]("{}")
        case None => "{}"
      }

      val configMap: Map[String, Map[String, String]] = utils.Logkafka.parseJsonStr(hostname, configJsonStr)

      val clientJsonStr = lct match {
        case Some(l) => l.client.getOrElse[String]("{}")
        case None => "{}"
      }

      val clientMap: Map[String, Map[String, String]]  = utils.Logkafka.parseJsonStr(hostname, clientJsonStr)

      val hostnameSet = configMap.keySet ++ clientMap.keySet
      val identitySet = if (!hostnameSet.isEmpty) {
        hostnameSet map { l => l -> ((if(!configMap.isEmpty) configMap.get(l) else None, if(!clientMap.isEmpty) clientMap.get(l) else None)) }
      } else { Set() }
      LogkafkaIdentity(hostname, lct.isDefined, identitySet.toMap)
    }
  }
}
