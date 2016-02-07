/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.actor.cluster

import java.util.concurrent.TimeUnit
import java.util.Properties

import akka.pattern._
import akka.actor.{ActorRef, Cancellable, ActorPath}
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import grizzled.slf4j.Logging
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.admin._
import kafka.coordinator.{GroupOverview, GroupSummary, MemberSummary}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.Utils
import kafka.manager._
import kafka.manager.base.cluster.BaseClusterQueryCommandActor
import kafka.manager.base.{LongRunningPoolActor, LongRunningPoolConfig}
import kafka.manager.features.{ClusterFeatures, KMPollConsumersFeature, KMDeleteTopicFeature}
import kafka.manager.model.ActorModel._
import kafka.manager.model.{ClusterContext, KafkaVersion, Kafka_0_8_1_1}
import kafka.manager.utils.ZkUtils
import kafka.manager.utils.zero81.{PreferredReplicaLeaderElectionCommand, ReassignPartitionCommand}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.cache._
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
 * @author hiral
 */
import kafka.manager.utils._

import scala.collection.JavaConverters._

case class ConsumerInstanceSubscriptions private(id: String, subs: Map[String, Int])

object ConsumerInstanceSubscriptions extends Logging {
  
  //{"version":1,"subscription":{"DXSPreAgg":1},"pattern":"static","timestamp":"1443578242654"}
  def apply(consumer: String, id: String, jsonString: String) : ConsumerInstanceSubscriptions = {
    import org.json4s.jackson.JsonMethods.parse
    import org.json4s.scalaz.JsonScalaz.field
    val json = parse(jsonString)
    val subs: Map[String, Int] = field[Map[String,Int]]("subscription")(json).fold({ e =>
      error(s"[consumer=$consumer] Failed to parse consumer instance subscriptions : $id : $jsonString"); Map.empty}, identity)
    new ConsumerInstanceSubscriptions(id, subs)
  }
}

trait OffsetCache extends Logging {
  
  def getKafkaVersion: KafkaVersion
  
  def getCacheTimeoutSecs: Int

  def getSimpleConsumerSocketTimeoutMillis: Int

  protected[this] implicit def ec: ExecutionContext
  
  protected[this] implicit def cf: ClusterFeatures
  
  protected[this] val loadOffsets: Boolean

  // Caches a map of partitions to offsets at a key that is the topic's name.
  private[this] val partitionOffsetsCache: LoadingCache[String, Future[PartitionOffsetsCapture]] = CacheBuilder.newBuilder()
    .expireAfterWrite(getCacheTimeoutSecs,TimeUnit.SECONDS) // TODO - update more or less often maybe, or make it configurable
    .build(
      new CacheLoader[String,Future[PartitionOffsetsCapture]] {
        def load(topic: String): Future[PartitionOffsetsCapture] = {
          loadPartitionOffsets(topic)
        }
      }
    )

  // Get the latest offsets for the partitions of the topic,
  // Code based off of the GetOffsetShell tool in kafka.tools, kafka 0.8.2.1
  private[this] def loadPartitionOffsets(topic: String): Future[PartitionOffsetsCapture] = {
    // Get partition leader broker information
    val optPartitionsWithLeaders : Option[List[(Int, Option[BrokerIdentity])]] = getTopicPartitionLeaders(topic)

    val clientId = "partitionOffsetGetter"
    val time = -1
    val nOffsets = 1
    val simpleConsumerBufferSize = 256 * 1024

    val partitionsByBroker = optPartitionsWithLeaders.map {
      listOfPartAndBroker => listOfPartAndBroker.collect {
        case (part, broker) if broker.isDefined => (broker.get, part)
      }.groupBy(_._1)
    }

    def getSimpleConsumer(bi: BrokerIdentity) =
      new SimpleConsumer(bi.host, bi.port, getSimpleConsumerSocketTimeoutMillis, 256 * 1024, clientId)

    // Get the latest offset for each partition
    val futureMap: Future[PartitionOffsetsCapture] = {
      partitionsByBroker.fold[Future[PartitionOffsetsCapture]]{
        Future.failed(new IllegalArgumentException(s"Do not have partitions and their leaders for topic $topic"))
      } { partitionsWithLeaders =>
        try {
          val listOfFutures = partitionsWithLeaders.toList.map(tpl => (getSimpleConsumer(tpl._1), tpl._2)).map {
            case (simpleConsumer, parts) =>
              val f: Future[Map[Int, Option[Long]]] = Future {
                try {
                  val topicAndPartitions = parts.map(tpl => (TopicAndPartition(topic, tpl._2), PartitionOffsetRequestInfo(time, nOffsets)))
                  val request = OffsetRequest(topicAndPartitions.toMap)
                  simpleConsumer.getOffsetsBefore(request).partitionErrorAndOffsets.map(tpl => (tpl._1.asTuple._2, tpl._2.offsets.headOption))
                } finally {
                  simpleConsumer.close()
                }
              }
              f.recover { case t =>
                error(s"[topic=$topic] An error has occurred while getting topic offsets from broker $parts", t)
                Map.empty[Int, Option[Long]]
              }
          }
          val result: Future[Map[Int, Option[Long]]] = Future.sequence(listOfFutures).map(_.foldRight(Map.empty[Int, Option[Long]])((b, a) => b ++ a))
          result.map(m => PartitionOffsetsCapture(System.currentTimeMillis(), m.mapValues(_.getOrElse(0L))))
        } 
        catch {
          case e: Exception =>
            error(s"Failed to get offsets for topic $topic", e)
            Future.failed(e)
        }
      }
    }

    futureMap onFailure {
      case t => error(s"[topic=$topic] An error has occurred while getting topic offsets", t)
    }
    futureMap
  }

  private[this] def emptyPartitionOffsetsCapture: Future[PartitionOffsetsCapture] = Future.successful(PartitionOffsetsCapture(System.currentTimeMillis(), Map()))
  
  protected def getTopicPartitionLeaders(topic: String) : Option[List[(Int, Option[BrokerIdentity])]]

  def getTopicDescription(topic: String, interactive: Boolean) : Option[TopicDescription]

  protected def readConsumerOffsetByTopicPartition(consumer: String, topic: String, tpi: Map[Int, TopicPartitionIdentity]) : Map[Int, Long]
  
  protected def readConsumerOwnerByTopicPartition(consumer: String, topic: String, tpi: Map[Int, TopicPartitionIdentity]) : Map[Int, String]

  protected def getConsumerTopicsFromIds(consumer: String) : Set[String]

  protected def getConsumerTopicsFromOffsets(consumer: String) : Set[String]

  protected def getConsumerTopicsFromOwners(consumer: String) : Set[String]

  /*
  protected def readConsumerOffsetByTopicPartitionFromKafka(consumer: String, topic: String, tpi: Map[Int, TopicPartitionIdentity]) : Map[Int, Long] = {
    val topicPartitions = tpi.keys.map(part => TopicAndPartition(topic, part))
    val channel = ClientUtils.channelToOffsetManager(group, zkClient, channelSocketTimeoutMs, channelRetryBackoffMs)

    debug("Sending offset fetch request to coordinator %s:%d.".format(channel.host, channel.port))
    channel.send(OffsetFetchRequest(group, topicPartitions))
    val offsetFetchResponse = OffsetFetchResponse.readFrom(channel.receive().buffer)
    debug("Received offset fetch response %s.".format(offsetFetchResponse))

    offsetFetchResponse.requestInfo.foreach { case (topicAndPartition, offsetAndMetadata) =>
      if (offsetAndMetadata == OffsetMetadataAndError.NoOffset) {
        val topicDirs = new ZKGroupTopicDirs(group, topicAndPartition.topic)
        // this group may not have migrated off zookeeper for offsets storage (we don't expose the dual-commit option in this tool
        // (meaning the lag may be off until all the consumers in the group have the same setting for offsets storage)
        try {
          val offset = ZkUtils.readData(zkClient, topicDirs.consumerOffsetDir + "/%d".format(topicAndPartition.partition))._1.toLong
          offsetMap.put(topicAndPartition, offset)
        } catch {
          case z: ZkNoNodeException =>
            if(ZkUtils.pathExists(zkClient,topicDirs.consumerOffsetDir))
              offsetMap.put(topicAndPartition,-1)
            else
              throw z
        }
      }
      else if (offsetAndMetadata.error == ErrorMapping.NoError)
        offsetMap.put(topicAndPartition, offsetAndMetadata.offset)
      else {
        println("Could not fetch offset for %s due to %s.".format(topicAndPartition, ErrorMapping.exceptionFor(offsetAndMetadata.error)))
      }
    }
    channel.disconnect()
  }*/

  protected def getConsumerTopics(consumer: String) : Set[String] = {
    getConsumerTopicsFromOffsets(consumer) ++ getConsumerTopicsFromOwners(consumer) ++ getConsumerTopicsFromIds(consumer)
  }

  def start()
  
  def stop()

  def getTopicPartitionOffsets(topic: String, interactive: Boolean) : Future[PartitionOffsetsCapture] = {
    if(interactive || loadOffsets) {
      partitionOffsetsCache.get(topic)
    } else {
      emptyPartitionOffsetsCapture
    }
  }
  
  def lastUpdateMillis : Long
  
  final def getConsumerDescription(consumer: String) : ConsumerDescription = {
    val consumerTopics: Set[String] = getKafkaVersion match {
      case Kafka_0_8_1_1 => getConsumerTopicsFromOffsets(consumer)
      case _ => getConsumerTopicsFromOffsets(consumer) ++ getConsumerTopicsFromOwners(consumer)
    }

    val topicDescriptions: Map[String, ConsumedTopicDescription] = consumerTopics.map { topic =>
          val topicDesc = getConsumedTopicDescription(consumer, topic, false)
          (topic, topicDesc)
        }.toMap
    ConsumerDescription(consumer, topicDescriptions)
  }
  
  final def getConsumedTopicDescription(consumer:String, topic:String, interactive: Boolean) : ConsumedTopicDescription = {
    val optTopic = getTopicDescription(topic, interactive)
    val optTpi = optTopic.map(TopicIdentity.getTopicPartitionIdentity(_, None))
    val partitionOffsets = for {
      td <- optTopic
      tpi <- optTpi
    } yield {
      readConsumerOffsetByTopicPartition(consumer, topic, tpi)
    }
    val partitionOwners = for {
      td <- optTopic
      tpi <- optTpi
    } yield {
      readConsumerOwnerByTopicPartition(consumer, topic, tpi)
    }

    val numPartitions: Int = math.max(optTopic.flatMap(_.partitionState.map(_.size)).getOrElse(0),
      partitionOffsets.map(_.size).getOrElse(0))
    ConsumedTopicDescription(consumer, topic, numPartitions, optTopic, partitionOwners, partitionOffsets)
  }
  
  def getConsumerList(adminClient: AdminClient, isCloseClient: Boolean): ConsumerList

  def getConsumerListByKafka(adminClient: AdminClient): IndexedSeq[String] = {
    var consumerGroupList: List[String] = List()
    var groupOverviewList: List[GroupOverview] = adminClient.listAllConsumerGroupsFlattened()
    groupOverviewList match {
      case Nil => IndexedSeq.empty
      case l: List[GroupOverview] => {
        l.foreach {
          x => consumerGroupList = x.groupId :: consumerGroupList
        }
      }
    }
    groupOverviewList = null
    consumerGroupList.toIndexedSeq
  }
}

case class OffsetCacheActive(curator: CuratorFramework,
                                  clusterContext: ClusterContext, 
                                  partitionLeaders: String => Option[List[(Int, Option[BrokerIdentity])]],
                                  topicDescriptions: (String, Boolean) => Option[TopicDescription],
                                  cacheTimeoutSecs: Int,
                                  socketTimeoutMillis: Int,
                                  kafkaVersion: KafkaVersion)
                                 (implicit protected[this] val ec: ExecutionContext, val cf: ClusterFeatures) extends OffsetCache {

  def getKafkaVersion: KafkaVersion = kafkaVersion

  def getCacheTimeoutSecs: Int = cacheTimeoutSecs

  def getSimpleConsumerSocketTimeoutMillis: Int = socketTimeoutMillis

  val loadOffsets = featureGateFold(KMPollConsumersFeature)(false, true)

  private[this] val consumersTreeCacheListener = new TreeCacheListener {
    override def childEvent(client: CuratorFramework, event: TreeCacheEvent): Unit = {
      event.getType match {
        case TreeCacheEvent.Type.INITIALIZED | TreeCacheEvent.Type.NODE_ADDED |
             TreeCacheEvent.Type.NODE_REMOVED | TreeCacheEvent.Type.NODE_UPDATED =>
          consumersTreeCacheLastUpdateMillis = System.currentTimeMillis()
        case _ =>
        //do nothing
      }
    }
  }
  
  private[this] val consumersTreeCache = new TreeCache(curator, ZkUtils.ConsumersPath)
  
  @volatile
  private[this] var consumersTreeCacheLastUpdateMillis : Long = System.currentTimeMillis()

  private[this] def withConsumersTreeCache[T](fn: TreeCache => T) : Option[T] = {
    Option(fn(consumersTreeCache))
  }

  protected def getTopicPartitionLeaders(topic: String) : Option[List[(Int, Option[BrokerIdentity])]] = partitionLeaders(topic)

  def getTopicDescription(topic: String, interactive: Boolean) : Option[TopicDescription] = topicDescriptions(topic, interactive)
  
  def start():  Unit = {
    info("Starting consumers tree cache...")
    consumersTreeCache.start()

    info("Adding consumers tree cache listener...")
    consumersTreeCache.getListenable.addListener(consumersTreeCacheListener)
  }
  
  def stop(): Unit = {
    info("Removing consumers tree cache listener...")
    Try(consumersTreeCache.getListenable.removeListener(consumersTreeCacheListener))
    
    info("Shutting down consumers tree cache...")
    Try(consumersTreeCache.close())
  }

  def lastUpdateMillis : Long = consumersTreeCacheLastUpdateMillis

  protected def readConsumerOffsetByTopicPartition(consumer: String, topic: String, tpi: Map[Int, TopicPartitionIdentity]) : Map[Int, Long] = {
    tpi.map {
      case (p, _) =>
        val offsetPath = "%s/%s/%s/%s/%s".format(ZkUtils.ConsumersPath, consumer, "offsets", topic, p)
        (p, Option(consumersTreeCache.getCurrentData(offsetPath)).flatMap(cd => Option(cd.getData)).map(asString).getOrElse("-1").toLong)
    }
    
  }
  
  protected def readConsumerOwnerByTopicPartition(consumer: String, topic: String, tpi: Map[Int, TopicPartitionIdentity]) : Map[Int, String] = {
    tpi.map {
      case (p, _) =>
        val offsetPath = "%s/%s/%s/%s/%s".format(ZkUtils.ConsumersPath, consumer, "owners", topic, p)
        (p, Option(consumersTreeCache.getCurrentData(offsetPath)).flatMap(cd => Option(cd.getData)).map(asString).getOrElse(""))
    }
  }

  protected def getConsumerTopicsFromIds(consumer: String) : Set[String] = {
    val zkPath = "%s/%s/%s".format(ZkUtils.ConsumersPath,consumer,"ids")
    Option(consumersTreeCache.getCurrentChildren(zkPath)).map(_.asScala.toMap.map {
      case (id, cd) => ConsumerInstanceSubscriptions.apply(consumer, id, Option(cd).map(_.getData).map(asString).getOrElse("{}"))
    }.map(_.subs.keys).flatten.toSet).getOrElse(Set.empty)
  }

  protected def getConsumerTopicsFromOffsets(consumer: String) : Set[String] = {
    val zkPath = "%s/%s/%s".format(ZkUtils.ConsumersPath,consumer,"offsets")
    Option(consumersTreeCache.getCurrentChildren(zkPath)).map(_.asScala.toMap.keySet).getOrElse(Set.empty)
  }

  protected def getConsumerTopicsFromOwners(consumer: String) : Set[String] = {
    val zkPath = "%s/%s/%s".format(ZkUtils.ConsumersPath,consumer,"owners")
    Option(consumersTreeCache.getCurrentChildren(zkPath)).map(_.asScala.toMap.keySet).getOrElse(Set.empty)
  }

  /*
  def getConsumerDescription(consumer: String) : Option[ConsumerDescription] = {
    val offsetPath = "%s/%s/%s".format(ZkUtils.ConsumersPath,consumer,"offsets")
    val topicOffsetOption : Option[Map[String, ChildData]] = Option(consumersTreeCache.getCurrentChildren(offsetPath)).map(_.asScala.toMap)

    val topicDescriptions: Option[Map[String, ConsumedTopicDescription]] =
      topicOffsetOption.map[List[(String, ConsumedTopicDescription)]] { topics: Map[String, ChildData] =>
        for {
          topicAndData: (String, ChildData) <- topics.toList
          topicDesc = getConsumedTopicDescription(consumer, topicAndData._1)
        } yield (topicAndData._1, topicDesc)
      }.map(_.toMap)

    topicDescriptions.map(ConsumerDescription(consumer, _))
  }*/

  def getConsumerList(adminClient: AdminClient, isCloseClient: Boolean): ConsumerList = {
    var sumConsumerList = getConsumerListByKafka(adminClient)
    withConsumersTreeCache { cache =>
      cache.getCurrentChildren(ZkUtils.ConsumersPath)
    }.fold {
      if (isCloseClient) {
        adminClient.close()
      }
      ConsumerList(IndexedSeq.empty.++:(sumConsumerList), clusterContext)
    } { data: java.util.Map[String, ChildData] =>
      if (isCloseClient) {
        adminClient.close()
      }
      val filteredList: IndexedSeq[String] = data.asScala.filter{
        case (consumer, childData) =>
          if (clusterContext.config.filterConsumers)
          // Defining "inactive consumer" as a consumer that is missing one of three children ids/ offsets/ or owners/
            childData.getStat.getNumChildren > 2
          else true
      }.keySet.toIndexedSeq
      ConsumerList(filteredList.++:(sumConsumerList).toSet.toIndexedSeq, clusterContext)
    }
  }
}

case class OffsetCachePassive(curator: CuratorFramework,
                             clusterContext: ClusterContext,
                             partitionLeaders: String => Option[List[(Int, Option[BrokerIdentity])]],
                             topicDescriptions: (String, Boolean) => Option[TopicDescription],
                             cacheTimeoutSecs: Int,
                             socketTimeoutMillis: Int,
                             kafkaVersion: KafkaVersion)
                            (implicit protected[this] val ec: ExecutionContext, val cf: ClusterFeatures) extends OffsetCache {

  def getKafkaVersion: KafkaVersion = kafkaVersion

  def getCacheTimeoutSecs: Int = cacheTimeoutSecs

  def getSimpleConsumerSocketTimeoutMillis: Int = socketTimeoutMillis

  val loadOffsets = featureGateFold(KMPollConsumersFeature)(false, true)

  private[this] val consumersPathChildrenCacheListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case PathChildrenCacheEvent.Type.INITIALIZED | PathChildrenCacheEvent.Type.CHILD_ADDED |
             PathChildrenCacheEvent.Type.CHILD_REMOVED | PathChildrenCacheEvent.Type.CHILD_UPDATED =>
          consumersTreeCacheLastUpdateMillis = System.currentTimeMillis()
        case _ =>
        //do nothing
      }
    }
  }

  private[this] val consumersPathChildrenCache = new PathChildrenCache(curator, ZkUtils.ConsumersPath, true)

  @volatile
  private[this] var consumersTreeCacheLastUpdateMillis : Long = System.currentTimeMillis()

  private[this] def withConsumersPathChildrenCache[T](fn: PathChildrenCache => T) : Option[T] = {
    Option(fn(consumersPathChildrenCache))
  }

  protected def getTopicPartitionLeaders(topic: String) : Option[List[(Int, Option[BrokerIdentity])]] = partitionLeaders(topic)

  def getTopicDescription(topic: String, interactive: Boolean) : Option[TopicDescription] = topicDescriptions(topic, interactive)

  def start():  Unit = {
    info("Starting consumers path children cache...")
    consumersPathChildrenCache.start(StartMode.BUILD_INITIAL_CACHE)

    info("Adding consumers path children cache listener...")
    consumersPathChildrenCache.getListenable.addListener(consumersPathChildrenCacheListener)
  }

  def stop(): Unit = {
    info("Removing consumers path children cache listener...")
    Try(consumersPathChildrenCache.getListenable.removeListener(consumersPathChildrenCacheListener))

    info("Shutting down consumers path children cache...")
    Try(consumersPathChildrenCache.close())
  }

  def lastUpdateMillis : Long = consumersTreeCacheLastUpdateMillis

  protected def readConsumerOffsetByTopicPartition(consumer: String, topic: String, tpi: Map[Int, TopicPartitionIdentity]) : Map[Int, Long] = {
    tpi.map {
      case (p, _) =>
        val offsetPath = "%s/%s/%s/%s/%s".format(ZkUtils.ConsumersPath, consumer, "offsets", topic, p)
        (p, ZkUtils.readDataMaybeNull(curator, offsetPath)._1.map(_.toLong).getOrElse(-1L))
    }
  }

  protected def readConsumerOwnerByTopicPartition(consumer: String, topic: String, tpi: Map[Int, TopicPartitionIdentity]) : Map[Int, String] = {
    tpi.map {
      case (p, _) =>
        val ownerPath = "%s/%s/%s/%s/%s".format(ZkUtils.ConsumersPath, consumer, "owners", topic, p)
        (p, ZkUtils.readDataMaybeNull(curator, ownerPath)._1.orNull)
    }.filter(_._2 != null)
  }

  protected def getConsumerTopicsFromIds(consumer: String) : Set[String] = {
    val zkPath = "%s/%s/%s".format(ZkUtils.ConsumersPath,consumer,"ids")
    val ids = Try(Option(curator.getChildren.forPath(zkPath)).map(_.asScala.toIterable)).toOption.flatten.getOrElse(Iterable.empty)
    val topicList : Iterable[Iterable[String]] = for {
      id <- ids
      idPath = "%s/%s".format(zkPath, id)
    } yield {
      ZkUtils.readDataMaybeNull(
        curator, idPath)._1.map(ConsumerInstanceSubscriptions.apply(consumer, id, _)).map(_.subs.keys).getOrElse(Iterable.empty)
    }
    topicList.flatten.toSet
  }

  protected def getConsumerTopicsFromOffsets(consumer: String) : Set[String] = {
    val zkPath = "%s/%s/%s".format(ZkUtils.ConsumersPath,consumer,"offsets")
    Try(Option(curator.getChildren.forPath(zkPath)).map(_.asScala.toSet)).toOption.flatten.getOrElse(Set.empty)
  }

  protected def getConsumerTopicsFromOwners(consumer: String) : Set[String] = {
    val zkPath = "%s/%s/%s".format(ZkUtils.ConsumersPath,consumer,"owners")
    Try(Option(curator.getChildren.forPath(zkPath)).map(_.asScala.toSet)).toOption.flatten.getOrElse(Set.empty)
  }
  
  def getConsumerList(adminClient: AdminClient, isCloseClient: Boolean): ConsumerList = {
    val sumConsumerList = getConsumerListByKafka(adminClient)
    withConsumersPathChildrenCache { cache =>
      val currentData = cache.getCurrentData
      currentData
    }.fold {
      if (isCloseClient) {
        adminClient.close()
      }
      ConsumerList(IndexedSeq.empty.++:(sumConsumerList), clusterContext)
    } { data: java.util.List[ChildData] =>
      if (isCloseClient) {
        adminClient.close()
      }
      ConsumerList(data.asScala.map(cd => cd.getPath.split("/").last).toIndexedSeq.++:(sumConsumerList).toSet.toIndexedSeq, clusterContext)
    }
  }
}

case class KafkaStateActorConfig(curator: CuratorFramework,
                                 clusterContext: ClusterContext,
                                 longRunningPoolConfig: LongRunningPoolConfig,
                                 partitionOffsetCacheTimeoutSecs: Int, simpleConsumerSocketTimeoutMillis: Int)
class KafkaStateActor(config: KafkaStateActorConfig) extends BaseClusterQueryCommandActor with LongRunningPoolActor {

  private val consumerInfoByKafka: Duration = 10 seconds

  protected implicit val clusterContext: ClusterContext = config.clusterContext

  protected implicit val cf: ClusterFeatures = clusterContext.clusterFeatures

  override protected def longRunningPoolConfig: LongRunningPoolConfig = config.longRunningPoolConfig

  override protected def longRunningQueueFull(): Unit = {
    log.error("Long running pool queue full, skipping!")
  }

  // e.g. /brokers/topics/analytics_content/partitions/0/state
  private[this] val topicsTreeCache = new TreeCache(config.curator,ZkUtils.BrokerTopicsPath)

  private[this] val topicsConfigPathCache = new PathChildrenCache(config.curator,ZkUtils.TopicConfigPath,true)

  private[this] val brokersPathCache = new PathChildrenCache(config.curator,ZkUtils.BrokerIdsPath,true)

  private[this] val adminPathCache = new PathChildrenCache(config.curator,ZkUtils.AdminPath,true)

  private[this] val deleteTopicsPathCache = new PathChildrenCache(config.curator, ZkUtils.DeleteTopicsPath,true)

  @volatile
  private[this] var topicsTreeCacheLastUpdateMillis : Long = System.currentTimeMillis()

  private[this] val topicsTreeCacheListener = new TreeCacheListener {
    override def childEvent(client: CuratorFramework, event: TreeCacheEvent): Unit = {
      event.getType match {
        case TreeCacheEvent.Type.INITIALIZED | TreeCacheEvent.Type.NODE_ADDED |
             TreeCacheEvent.Type.NODE_REMOVED | TreeCacheEvent.Type.NODE_UPDATED =>
          topicsTreeCacheLastUpdateMillis = System.currentTimeMillis()
        case _ =>
        //do nothing
      }
    }
  }

  @volatile
  private[this] var preferredLeaderElection : Option[PreferredReplicaElection] = None

  @volatile
  private[this] var reassignPartitions : Option[ReassignPartitions] = None

  private[this] val adminPathCacheListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      log.info(s"Got event : ${event.getType} path=${Option(event.getData).map(_.getPath)}")
      event.getType match {
        case PathChildrenCacheEvent.Type.INITIALIZED =>
          event.getInitialData.asScala.foreach { cd: ChildData =>
            updatePreferredLeaderElection(cd)
            updateReassignPartition(cd)
          }
        case PathChildrenCacheEvent.Type.CHILD_ADDED | PathChildrenCacheEvent.Type.CHILD_UPDATED =>
          updatePreferredLeaderElection(event.getData)
          updateReassignPartition(event.getData)
        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
          endPreferredLeaderElection(event.getData)
          endReassignPartition(event.getData)
        case _ =>
        //do nothing
      }
    }

    private[this] def updatePreferredLeaderElection(cd: ChildData): Unit = {
      if(cd != null && cd.getPath.endsWith(ZkUtils.PreferredReplicaLeaderElectionPath)) {
        Try {
          self ! KSUpdatePreferredLeaderElection(cd.getStat.getMtime, cd.getData)
        }
      }
    }

    private[this] def updateReassignPartition(cd: ChildData): Unit = {
      if(cd != null && cd.getPath.endsWith(ZkUtils.ReassignPartitionsPath)) {
        Try {
          self ! KSUpdateReassignPartition(cd.getStat.getMtime, cd.getData)
        }
      }
    }

    private[this] def endPreferredLeaderElection(cd: ChildData): Unit = {
      if(cd != null && cd.getPath.endsWith(ZkUtils.PreferredReplicaLeaderElectionPath)) {
        Try {
          self ! KSEndPreferredLeaderElection(cd.getStat.getMtime)
        }
      }
    }

    private[this] def endReassignPartition(cd: ChildData): Unit = {
      if(cd != null && cd.getPath.endsWith(ZkUtils.ReassignPartitionsPath)) {
        Try {
          self ! KSEndReassignPartition(cd.getStat.getMtime)
        }
      }
    }
  }
 
  private def createAdminClient(): AdminClient = {
      val targetBrokers : IndexedSeq[BrokerIdentity] = getBrokers
      var brokerListStr = ""
      targetBrokers.foreach {
        b => {
          brokerListStr += "%s:%d,".format(b.host, b.port)
        }
      }

      AdminClient.createSimplePlaintext(brokerListStr)
  }

  private[this] val offsetCache: OffsetCache = {
    if(config.clusterContext.config.activeOffsetCacheEnabled)
      new OffsetCacheActive(
        config.curator,
        config.clusterContext,
        getPartitionLeaders,
        getTopicDescription,
        config.partitionOffsetCacheTimeoutSecs,
        config.simpleConsumerSocketTimeoutMillis,
        config.clusterContext.config.version)(longRunningExecutionContext, cf)
    else
      new OffsetCachePassive(
        config.curator,
        config.clusterContext,
        getPartitionLeaders,
        getTopicDescription,
        config.partitionOffsetCacheTimeoutSecs,
        config .simpleConsumerSocketTimeoutMillis,
        config.clusterContext.config.version)(longRunningExecutionContext, cf)
  }

  private[this] var cancellable : Option[Cancellable] = None
  private[this] var consumerByKafkaFuture: Future[Option[List[ConsumerDescription]]] = null

  @scala.throws[Exception](classOf[Exception])
  override def preStart() = {
    log.info(config.toString)
    log.info("Started actor %s".format(self.path))
    log.info("Starting topics tree cache...")
    topicsTreeCache.start()
    log.info("Starting topics config path cache...")
    topicsConfigPathCache.start(StartMode.BUILD_INITIAL_CACHE)
    log.info("Starting brokers path cache...")
    brokersPathCache.start(StartMode.BUILD_INITIAL_CACHE)
    log.info("Starting admin path cache...")
    adminPathCache.start(StartMode.BUILD_INITIAL_CACHE)
    log.info("Starting delete topics path cache...")
    deleteTopicsPathCache.start(StartMode.BUILD_INITIAL_CACHE)

    log.info("Adding topics tree cache listener...")
    topicsTreeCache.getListenable.addListener(topicsTreeCacheListener)
    log.info("Adding admin path cache listener...")
    adminPathCache.getListenable.addListener(adminPathCacheListener)

    //the offset cache does not poll on its own so it can be started safely
    log.info("Starting offset cache...")
    offsetCache.start()

    cancellable = Some(
      context.system.scheduler.schedule(0 seconds,
        10 seconds,
        self,
        KSForceUpdateConsumerByKafka)(context.system.dispatcher,self)
    )
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

    log.info("Stopping offset cache...")
    Try(offsetCache.stop())

    log.info("Removing admin path cache listener...")
    Try(adminPathCache.getListenable.removeListener(adminPathCacheListener))
    log.info("Removing topics tree cache listener...")
    Try(topicsTreeCache.getListenable.removeListener(topicsTreeCacheListener))

    log.info("Shutting down delete topics path cache...")
    Try(deleteTopicsPathCache.close())
    log.info("Shutting down admin path cache...")
    Try(adminPathCache.close())
    log.info("Shutting down brokers path cache...")
    Try(brokersPathCache.close())
    log.info("Shutting down topics config path cache...")
    Try(topicsConfigPathCache.close())
    log.info("Shutting down topics tree cache...")
    Try(topicsTreeCache.close())

    super.postStop()
  }

  def getTopicZookeeperData(topic: String): Option[(Int,String)] = {
    val topicPath = "%s/%s".format(ZkUtils.BrokerTopicsPath,topic)
    Option(topicsTreeCache.getCurrentData(topicPath)).map( childData => (childData.getStat.getVersion,asString(childData.getData)))
  }

  def getTopicDescription(topic: String, interactive: Boolean) : Option[TopicDescription] = {
    for {
      description <- getTopicZookeeperData(topic)
      partitionsPath = "%s/%s/partitions".format(ZkUtils.BrokerTopicsPath, topic)
      partitions: Map[String, ChildData] <- Option(topicsTreeCache.getCurrentChildren(partitionsPath)).map(_.asScala.toMap)
      states : Map[String, String] = partitions flatMap { case (part, _) =>
        val statePath = s"$partitionsPath/$part/state"
        Option(topicsTreeCache.getCurrentData(statePath)).map(cd => (part, asString(cd.getData)))
      }
      partitionOffsets = offsetCache.getTopicPartitionOffsets(topic, interactive)
      topicConfig = getTopicConfigString(topic)
    } yield TopicDescription(topic, description, Option(states), partitionOffsets, topicConfig)
  }

  def getPartitionLeaders(topic: String) : Option[List[(Int, Option[BrokerIdentity])]] = {
    val partitionsPath = "%s/%s/partitions".format(ZkUtils.BrokerTopicsPath, topic)
    val partitions: Option[Map[String, ChildData]] = Option(topicsTreeCache.getCurrentChildren(partitionsPath)).map(_.asScala.toMap)
    val states : Option[Iterable[(String, String)]] =
      partitions.map[Iterable[(String,String)]]{ partMap: Map[String, ChildData] =>
        partMap.flatMap { case (part, _) =>
          val statePath = s"$partitionsPath/$part/state"
          Option(topicsTreeCache.getCurrentData(statePath)).map(cd => (part, asString(cd.getData)))
        }
      }
    val targetBrokers : IndexedSeq[BrokerIdentity] = getBrokers

    import org.json4s.jackson.JsonMethods.parse
    import org.json4s.scalaz.JsonScalaz.field
    states.map(_.map{case (part, state) =>
      val partition = part.toInt
      val descJson = parse(state)
      val leaderID = field[Int]("leader")(descJson).fold({ e =>
        log.error(s"[topic=$topic] Failed to get partitions from topic json $state"); 0}, identity)
      val leader = targetBrokers.find(_.id == leaderID)
      (partition, leader)
    }.toList)
  }

  private[this] def getTopicConfigString(topic: String) : Option[(Int,String)] = {
    val data: mutable.Buffer[ChildData] = topicsConfigPathCache.getCurrentData.asScala
    val result: Option[ChildData] = data.find(p => p.getPath.endsWith(topic))
    result.map(cd => (cd.getStat.getVersion,asString(cd.getData)))
  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case any: Any => log.warning("ksa : processActorResponse : Received unknown message: {}", any.toString)
    }
  }


  private[this] def getBrokers : IndexedSeq[BrokerIdentity] = {
    val data: mutable.Buffer[ChildData] = brokersPathCache.getCurrentData.asScala
    data.map { cd =>
      BrokerIdentity.from(nodeFromPath(cd.getPath).toInt, asString(cd.getData))
    }.filter { v =>
      v match {
        case scalaz.Failure(nel) =>
          log.error(s"Failed to parse broker config $nel")
          false
        case _ => true
      }
    }.collect {
      case scalaz.Success(bi) => bi
    }.toIndexedSeq.sortBy(_.id)
  }
  
  private[this] def asyncPipeToSender[T](fn: => T):  Unit = {
    implicit val ec = longRunningExecutionContext
    val result: Future[T] = Future {
      fn
    }
    result pipeTo sender
  }

  override def processQueryRequest(request: QueryRequest): Unit = {
    request match {
      case KSGetTopics =>
        val deleteSet: Set[String] = 
          featureGateFold(KMDeleteTopicFeature)(
          Set.empty,
          {
            val deleteTopicsData: mutable.Buffer[ChildData] = deleteTopicsPathCache.getCurrentData.asScala
            deleteTopicsData.map { cd =>
              nodeFromPath(cd.getPath)
            }.toSet
          })
        withTopicsTreeCache { cache =>
          cache.getCurrentChildren(ZkUtils.BrokerTopicsPath)
        }.fold {
          sender ! TopicList(IndexedSeq.empty, deleteSet, config.clusterContext)
        } { data: java.util.Map[String, ChildData] =>
          sender ! TopicList(data.asScala.keySet.toIndexedSeq, deleteSet, config.clusterContext)
        }

      case KSGetConsumers =>
        asyncPipeToSender {
          var client = createAdminClient()
          val consumerList: ConsumerList = offsetCache.getConsumerList(client, false)
          client.close()
          client = null
          consumerList
        }

      case KSGetTopicConfig(topic) =>
        sender ! TopicConfig(topic, getTopicConfigString(topic))

      case KSGetTopicDescription(topic) =>
        sender ! getTopicDescription(topic, false)

      case KSGetTopicDescriptions(topics) =>
        sender ! TopicDescriptions(topics.toIndexedSeq.flatMap(getTopicDescription(_, false)), topicsTreeCacheLastUpdateMillis)

      case KSGetConsumerDescription(consumer) =>
        asyncPipeToSender {
          var currentCD: ConsumerDescription = offsetCache.getConsumerDescription(consumer)

          var topicConsumerMap = currentCD.topics

          Await.ready(consumerByKafkaFuture, consumerInfoByKafka).value.get match {
            case Success(consumerDescriptionList) =>
              consumerDescriptionList.map {
                consumerListByKafka => 
                consumerListByKafka.filter(_.consumer == consumer).map {
                  filter => {
                      topicConsumerMap = topicConsumerMap.++:(filter.topics)
                  }
                }
              }
            case Failure(e) =>
              topicConsumerMap
          }
          
          ConsumerDescription(consumer, topicConsumerMap)
        }

      case KSGetConsumedTopicDescription(consumer, topic) =>
        asyncPipeToSender {
          var consumedTD = offsetCache.getConsumedTopicDescription(consumer, topic, true)

          Await.ready(consumerByKafkaFuture, consumerInfoByKafka).value.get match {
            case Success(consumerDescriptionList) =>
              consumerDescriptionList.map {
                consumerListByKafka => 
                  consumerListByKafka.filter(_.consumer == consumer).map {
                    filter => {
                      consumedTD = filter.topics.get(topic).getOrElse(consumedTD)
                    }
                  }
              }
            case Failure(e) =>
          } 

          consumedTD
        }

      case KSGetAllTopicDescriptions(lastUpdateMillisOption) =>
        val lastUpdateMillis = lastUpdateMillisOption.getOrElse(0L)
        //since we want to update offsets, let's do so if last update plus offset cache timeout is before current time
        if (topicsTreeCacheLastUpdateMillis > lastUpdateMillis || ((topicsTreeCacheLastUpdateMillis + (config.partitionOffsetCacheTimeoutSecs * 1000)) < System.currentTimeMillis())) {
          //we have option here since there may be no topics at all!
          withTopicsTreeCache {  cache: TreeCache =>
            cache.getCurrentChildren(ZkUtils.BrokerTopicsPath)
          }.fold {
            sender ! TopicDescriptions(IndexedSeq.empty, topicsTreeCacheLastUpdateMillis)
          } { data: java.util.Map[String, ChildData] =>
            sender ! TopicDescriptions(data.asScala.keys.toIndexedSeq.flatMap(getTopicDescription(_, false)), topicsTreeCacheLastUpdateMillis)
          }
        } // else no updates to send

      case KSGetAllConsumerDescriptions(lastUpdateMillisOption) =>
        val lastUpdateMillis = lastUpdateMillisOption.getOrElse(0L)
        if (offsetCache.lastUpdateMillis > lastUpdateMillis) {
          asyncPipeToSender {
            var client = createAdminClient()
            ConsumerDescriptions(offsetCache
              .getConsumerList(client, true)
              .list
              .flatMap(c => {

                var topicConsumerMap = offsetCache.getConsumerDescription(c).topics

                  Await.ready(consumerByKafkaFuture, consumerInfoByKafka).value.get match {
                    case Success(consumerDescriptionList) =>
                      consumerDescriptionList.map {
                        consumerListByKafka => 
                          consumerListByKafka.filter(_.consumer == c).map {
                            filter => {
                              topicConsumerMap = topicConsumerMap.++:(filter.topics)
                            }
                          }
                      }
                    case Failure(e) =>
                      topicConsumerMap
                  }

                  Option(ConsumerDescription(c, topicConsumerMap))
              }),
            offsetCache.lastUpdateMillis)
          }
        }

      case KSGetTopicsLastUpdateMillis =>
        sender ! topicsTreeCacheLastUpdateMillis

      case KSGetBrokers =>
        sender ! BrokerList(getBrokers, config.clusterContext)

      case KSGetPreferredLeaderElection =>
        sender ! preferredLeaderElection

      case KSGetReassignPartition =>
        sender ! reassignPartitions

      case KSForceUpdateConsumerByKafka =>
        consumerByKafkaFuture = null
        consumerByKafkaFuture = getConsumerDescriptionByKafka()

      case any: Any => log.warning("ksa : processQueryRequest : Received unknown message: {}", any.toString)
    }
  }

  override def processCommandRequest(request: CommandRequest): Unit = {
    request match {
      case KSUpdatePreferredLeaderElection(millis,json) =>
        safeExecute {
          val s: Set[TopicAndPartition] = PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(json)
          preferredLeaderElection.fold {
            //nothing there, add as new
            preferredLeaderElection = Some(PreferredReplicaElection(getDateTime(millis), s, None, config.clusterContext))
          } {
            existing =>
              existing.endTime.fold {
                //update without end? Odd, copy existing
                preferredLeaderElection = Some(existing.copy(topicAndPartition = existing.topicAndPartition ++ s))
              } { _ =>
                //new op started
                preferredLeaderElection = Some(PreferredReplicaElection(getDateTime(millis), s, None, config.clusterContext))
              }
          }
        }
      case KSUpdateReassignPartition(millis,json) =>
        safeExecute {
          val m : Map[TopicAndPartition, Seq[Int]] = ReassignPartitionCommand.parsePartitionReassignmentZkData(json)
          reassignPartitions.fold {
            //nothing there, add as new
            reassignPartitions = Some(ReassignPartitions(getDateTime(millis),m, None, config.clusterContext))
          } {
            existing =>
              existing.endTime.fold {
                //update without end? Odd, copy existing
                reassignPartitions = Some(existing.copy(partitionsToBeReassigned = existing.partitionsToBeReassigned ++ m))
              } { _ =>
                //new op started
                reassignPartitions = Some(ReassignPartitions(getDateTime(millis),m, None, config.clusterContext))
              }
          }
        }
      case KSEndPreferredLeaderElection(millis) =>
        safeExecute {
          preferredLeaderElection.foreach { existing =>
            preferredLeaderElection = Some(existing.copy(endTime = Some(getDateTime(millis))))
          }
        }
      case KSEndReassignPartition(millis) =>
        safeExecute {
          reassignPartitions.foreach { existing =>
            reassignPartitions = Some(existing.copy(endTime = Some(getDateTime(millis))))
          }
        }
      case any: Any => log.warning("ksa : processCommandRequest : Received unknown message: {}", any.toString)
    }
  }

  private[this] def getDateTime(millis: Long) : DateTime = new DateTime(millis,DateTimeZone.UTC)

  private[this] def safeExecute(fn: => Any) : Unit = {
    Try(fn) match {
      case Failure(t) =>
        log.error("Failed!",t)
      case Success(_) =>
      //do nothing
    }
  }

  private[this] def withTopicsTreeCache[T](fn: TreeCache => T) : Option[T] = {
    Option(fn(topicsTreeCache))
  }

  private[this] def getConsumerDescriptionByKafka(): Future[Option[List[ConsumerDescription]]] = {
    def createNewConsumer(group: String): KafkaConsumer[String, String] = {
      val properties = new Properties()
      val deserializer = (new StringDeserializer).getClass.getName
      val targetBrokers : IndexedSeq[BrokerIdentity] = getBrokers
      var brokerListStr = ""
      targetBrokers.foreach {
        b => {
          brokerListStr += "%s:%d,".format(b.host, b.port)
        }
      }
      properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerListStr)
      properties.put(ConsumerConfig.GROUP_ID_CONFIG, group)
      properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
      properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
      properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)

      new KafkaConsumer(properties)
    }

    implicit val ec = longRunningExecutionContext
    var consumerByKafkaFuture: Future[Option[List[ConsumerDescription]]] = Future {
      var client = createAdminClient()
      offsetCache.getConsumerListByKafka(client).toList match {
        case Nil => {
          client.close()
          client = null
          None 
        }
        case l: List[String] => {
          var descriptions: List[ConsumerDescription] = List()

          l.foreach { 
            groupId => {
              var comsumedTopicDescriptions: Map[String, ConsumedTopicDescription] = Map()

              val consumerSummaries = client.describeConsumerGroup(groupId)
              if (!consumerSummaries.isEmpty) {
                val consumer = createNewConsumer(groupId)

                consumerSummaries.foreach { consumerSummary =>
                  val topicPartitions = consumerSummary.assignment.map(tp => TopicAndPartition(tp.topic, tp.partition))
                  val partitionOffsets = topicPartitions.flatMap { topicPartition =>
                    Option(consumer.committed(new TopicPartition(topicPartition.topic, topicPartition.partition))).map { offsetAndMetadata =>
                      topicPartition -> offsetAndMetadata.offset
                    }
                  }.toMap

                  descriptions = descriptions.:+(ConsumerDescription(groupId,
                    topicPartitions.map {
                        tp => (tp.topic, tp.partition)
                      }.groupBy(_._1).map {
                      case (topic, tpl) => {
                        (topic,
                          ConsumedTopicDescription(groupId,
                            topic,
                            tpl.size,
                            offsetCache.getTopicDescription(topic, false),
                            Option(tpl.map {
                              p => (p._2, s"${consumerSummary.clientId}_${consumerSummary.clientHost}")
                            }.toMap),
                            Option(tpl.map {
                              p => (p._2, partitionOffsets.get(TopicAndPartition(topic, p._2)).get)
                            }.toMap)))
                      }
                    }))
                }//foreach
                consumer.close()
              }//isEmpty
            }//groupId
          }//foreach
          if (!descriptions.isEmpty) {
            client.close()
            client = null
            Some(descriptions)
          } else {
            client.close()
            client = null
            None
          }
        }//List
      }//match
    }//Future
    consumerByKafkaFuture.recover { case t => None }

    consumerByKafkaFuture
  }
}

