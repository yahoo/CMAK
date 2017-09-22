/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.actor.cluster

import java.io.Closeable
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.{ConcurrentLinkedDeque, TimeUnit}

import akka.actor.{ActorContext, ActorPath, ActorRef, Props}
import akka.pattern._
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import grizzled.slf4j.Logging
import kafka.admin.AdminClient
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.consumer._
import kafka.coordinator.{GroupMetadataKey, GroupSummary, OffsetKey}
import kafka.manager._
import kafka.manager.base.cluster.{BaseClusterQueryActor, BaseClusterQueryCommandActor}
import kafka.manager.base.{LongRunningPoolActor, LongRunningPoolConfig}
import kafka.manager.features.{ClusterFeatures, KMDeleteTopicFeature, KMPollConsumersFeature}
import kafka.manager.model.ActorModel._
import kafka.manager.model._
import kafka.manager.utils.ZkUtils
import kafka.manager.utils.zero81.{PreferredReplicaLeaderElectionCommand, ReassignPartitionCommand}
import kafka.manager.utils.zero90.{GroupMetadata, MemberMetadata}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.cache._
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, KafkaConsumer}
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * @author hiral
 */
import kafka.manager.utils._

import scala.collection.JavaConverters._

case class KafkaAdminClientActorConfig(clusterContext: ClusterContext, longRunningPoolConfig: LongRunningPoolConfig, kafkaStateActorPath: ActorPath, consumerProperties: Option[Properties])
case class KafkaAdminClientActor(config: KafkaAdminClientActorConfig) extends BaseClusterQueryActor with LongRunningPoolActor {

  private[this] var adminClientOption : Option[AdminClient] = None

  protected implicit val clusterContext: ClusterContext = config.clusterContext
  override protected def longRunningPoolConfig: LongRunningPoolConfig = config.longRunningPoolConfig

  override protected def longRunningQueueFull(): Unit = {
    log.error("Long running pool queue full, skipping!")
  }

  @scala.throws[Exception](classOf[Exception])
  override def preStart() = {
    super.preStart()
    log.info(config.toString)
  }

  @scala.throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.error(reason, "Restarting due to [{}] when processing [{}]",
      reason.getMessage, message.getOrElse(""))
    super.preRestart(reason, message)
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info("Closing admin client...")
    Try(adminClientOption.foreach(_.close()))
    log.info("Stopped actor %s".format(self.path))
  }

  private def createAdminClient(bl: BrokerList): AdminClient = {
    val targetBrokers : IndexedSeq[BrokerIdentity] = bl.list
    val brokerListStr: String = targetBrokers.map {
      b =>
        val port = b.endpoints(config.clusterContext.config.securityProtocol)
        s"${b.host}:$port"
    }.mkString(",")
    val props = new Properties()
    config.consumerProperties.foreach {
      cp => props.putAll(cp)
    }
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.clusterContext.config.securityProtocol.stringId)
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerListStr)
    log.info(s"Creating admin client with security protocol=${config.clusterContext.config.securityProtocol.stringId} , broker list : $brokerListStr")
    AdminClient.create(props)
  }

  override def processQueryRequest(request: QueryRequest): Unit = {
    if(adminClientOption.isEmpty) {
      context.actorSelection(config.kafkaStateActorPath).tell(KSGetBrokers, self)
      log.error(s"AdminClient not initialized yet, cannot process request : $request")
    } else {
      implicit val ec = longRunningExecutionContext
      request match {
        case KAGetGroupSummary(groupList: Seq[String], enqueue: java.util.Queue[(String, kafka.coordinator.GroupSummary)]) =>
          Future {
            groupList.foreach {
              group =>
                try {
                  adminClientOption.foreach {
                    client =>
                      val summary = client.describeGroup(group)
                      if(summary != null) {
                        enqueue.offer(group -> summary)
                      }
                  }
                } catch {
                  case e: Exception =>
                    log.error(e, s"Failed to get group summary with admin client : $group")
                    log.error(e, s"Forcing new admin client initialization...")
                    Try { adminClientOption.foreach(_.close()) }
                    adminClientOption = None
                }
            }
          }
        case any: Any => log.warning("kac : processQueryRequest : Received unknown message: {}", any.toString)
      }
    }

  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case bl: BrokerList =>
        if(bl.list.nonEmpty) {
          Try {
            adminClientOption = Option(createAdminClient(bl))
          }.logError(s"Failed to create admin client with brokerlist : $bl")
        }
      case any: Any => log.warning("kac : processActorResponse : Received unknown message: {}", any.toString)
    }
  }
}

class KafkaAdminClient(context: => ActorContext, adminClientActorPath: ActorPath) {
  def enqueueGroupMetadata(groupList: Seq[String], queue: java.util.Queue[(String, GroupSummary)]) : Unit = {
    Try {
      context.actorSelection(adminClientActorPath).tell(KAGetGroupSummary(groupList, queue), ActorRef.noSender)
    }
  }
}


object KafkaManagedOffsetCache {
  val supportedVersions: Set[KafkaVersion] = Set(Kafka_0_8_2_0, Kafka_0_8_2_1, Kafka_0_8_2_2, Kafka_0_9_0_0, Kafka_0_9_0_1, Kafka_0_10_0_0, Kafka_0_10_0_1, Kafka_0_10_1_0, Kafka_0_10_1_1, Kafka_0_10_2_0, Kafka_0_10_2_1, Kafka_0_11_0_0)
  val ConsumerOffsetTopic = "__consumer_offsets"

  def isSupported(version: KafkaVersion) : Boolean = {
    supportedVersions(version)
  }
}

case class KafkaManagedOffsetCache(clusterContext: ClusterContext
                                   , adminClient: KafkaAdminClient
                                   , consumerProperties: Option[Properties]
                                   , bootstrapBrokerList: BrokerList
                                   , groupMemberMetadataCheckMillis: Int = 30000
                                    ) extends Runnable with Closeable with Logging {
  val groupTopicPartitionOffsetMap = new TrieMap[(String, String, Int), OffsetAndMetadata]()
  val topicConsumerSetMap = new TrieMap[String, mutable.Set[String]]()
  val consumerTopicSetMap = new TrieMap[String, mutable.Set[String]]()
  val groupTopicPartitionMemberMap = new TrieMap[(String, String, Int), MemberMetadata]()

  private[this] val queue = new ConcurrentLinkedDeque[(String, GroupSummary)]()

  @volatile
  private[this] var lastUpdateTimeMillis : Long = 0

  private[this] var lastGroupMemberMetadataCheckMillis : Long = System.currentTimeMillis()

  import KafkaManagedOffsetCache._
  import kafka.manager.utils.zero90.GroupMetadataManager._

  require(isSupported(clusterContext.config.version), s"Kafka version not support : ${clusterContext.config}")

  @volatile
  private[this] var shutdown: Boolean = false

  private[this] def createKafkaConsumer(): Consumer[Array[Byte], Array[Byte]] = {
    val hostname = InetAddress.getLocalHost.getHostName
    val brokerListStr: String = bootstrapBrokerList.list.map {
      b =>
        val port = b.endpoints(clusterContext.config.securityProtocol)
        s"${b.host}:$port"
    }.mkString(",")
    val props: Properties = new Properties()
    props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, s"KMOffsetCache-$hostname")
    props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerListStr)
    props.put(org.apache.kafka.clients.consumer.ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    consumerProperties.foreach {
      cp => props.putAll(cp)
    }
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, clusterContext.config.securityProtocol.stringId)
    Try {
      info("Constructing new kafka consumer client using these properties: ")
      props.asScala.foreach {
        case (k, v) => info(s"$k=$v")
      }
    }
    new KafkaConsumer[Array[Byte], Array[Byte]](props)
  }

  private[this] def performGroupMetadataCheck() : Unit = {
    val currentMillis = System.currentTimeMillis()
    if((lastGroupMemberMetadataCheckMillis + groupMemberMetadataCheckMillis) < currentMillis) {
      val diff = groupTopicPartitionOffsetMap.keySet.filterNot(groupTopicPartitionMemberMap.contains)
      if(diff.nonEmpty) {
        val groupsToBackfill = diff.map(_._1).toSeq
        info(s"Backfilling group metadata for $groupsToBackfill")
        adminClient.enqueueGroupMetadata(groupsToBackfill, queue)
      }
      lastGroupMemberMetadataCheckMillis = System.currentTimeMillis()
      lastUpdateTimeMillis = System.currentTimeMillis()
    }
  }

  private[this] def dequeueAndProcessBackFill(): Unit = {
    while(!queue.isEmpty) {
      val (groupId, summary) = queue.pop()
      summary.members.foreach {
        member =>
          try {
            val mm = MemberMetadata.from(groupId, summary, member)
            mm.assignment.foreach {
              case (topic, part) =>
                val k = (groupId, topic, part)
                //only add it if it hasn't already been added through a new update via the offset topic
                if(!groupTopicPartitionMemberMap.contains(k)) {
                  groupTopicPartitionMemberMap += (groupId, topic, part) -> mm
                }
            }
          } catch {
            case e: Exception =>
              error(s"Failed to get member metadata from group summary and member summary : $groupId : $member", e)
          }
      }
    }
  }

  override def run(): Unit = {
    if(!shutdown) {
      for {
        consumer <- Try {
          val consumer = createKafkaConsumer()
          consumer.subscribe(java.util.Arrays.asList(KafkaManagedOffsetCache.ConsumerOffsetTopic))
          consumer
        }.logError(s"Failed to create consumer for offset topic for cluster ${clusterContext.config.name}")
      } {
        try {
          info(s"Consumer created for kafka offset topic consumption for cluster ${clusterContext.config.name}")
          while (!shutdown) {
            try {
              try {
                dequeueAndProcessBackFill()
                performGroupMetadataCheck()
              } catch {
                case e: Exception =>
                  error("Failed to backfill group metadata", e)
              }

              val records: ConsumerRecords[Array[Byte], Array[Byte]] = consumer.poll(100)
              val iterator = records.iterator()
              while (iterator.hasNext) {
                val record = iterator.next()
                readMessageKey(ByteBuffer.wrap(record.key())) match {
                  case OffsetKey(version, key) =>
                    val value: OffsetAndMetadata = readOffsetMessageValue(ByteBuffer.wrap(record.value()))
                    groupTopicPartitionOffsetMap += (key.group, key.topicPartition.topic, key.topicPartition.partition) -> value
                    val topic = key.topicPartition.topic
                    val group = key.group
                    val consumerSet = {
                      if (topicConsumerSetMap.contains(topic)) {
                        topicConsumerSetMap(topic)
                      } else {
                        val s = new mutable.TreeSet[String]()
                        topicConsumerSetMap += topic -> s
                        s
                      }
                    }
                    consumerSet += group

                    val topicSet = {
                      if (consumerTopicSetMap.contains(group)) {
                        consumerTopicSetMap(group)
                      } else {
                        val s = new mutable.TreeSet[String]()
                        consumerTopicSetMap += group -> s
                        s
                      }
                    }
                    topicSet += topic
                  case GroupMetadataKey(version, key) =>
                    val value: GroupMetadata = readGroupMessageValue(key, ByteBuffer.wrap(record.value()))
                    value.allMemberMetadata.foreach {
                      mm =>
                        mm.assignment.foreach {
                          case (topic, part) =>
                            groupTopicPartitionMemberMap += (key, topic, part) -> mm
                        }
                    }
                }
              }
              lastUpdateTimeMillis = System.currentTimeMillis()
            } catch {
              case e: Exception =>
                warn(s"Failed to process a message from offset topic on cluster ${clusterContext.config.name}!", e)
            }
          }
        } finally {
          info(s"Shutting down consumer for $ConsumerOffsetTopic on cluster ${clusterContext.config.name}")
          Try(consumer.close())
        }
      }
    }
    info(s"KafkaManagedOffsetCache shut down for cluster ${clusterContext.config.name}")
  }

  def close(): Unit = {
    this.shutdown = true
  }

  def getOffset(group: String, topic: String, part:Int) : Option[Long] = {
    groupTopicPartitionOffsetMap.get((group, topic, part)).map(_.offset)
  }

  def getOwner(group: String, topic: String, part:Int) : Option[String] = {
    groupTopicPartitionMemberMap.get((group, topic, part)).map(mm => s"${mm.memberId}:${mm.clientHost}")
  }

  def getConsumerTopics(group: String) : Set[String] = consumerTopicSetMap.get(group).map(_.toSet).getOrElse(Set.empty)
  def getTopicConsumers(topic: String) : Set[String] = topicConsumerSetMap.get(topic).map(_.toSet).getOrElse(Set.empty)
  def getConsumers : IndexedSeq[String] = consumerTopicSetMap.keys.toIndexedSeq
  def getLastUpdateTimeMillis: Long = lastUpdateTimeMillis
}

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

  def consumerProperties: Option[Properties]

  def kafkaAdminClient: KafkaAdminClient

  def clusterContext: ClusterContext

  def getKafkaVersion: KafkaVersion
  
  def getCacheTimeoutSecs: Int

  def getSimpleConsumerSocketTimeoutMillis: Int

  protected[this] implicit def ec: ExecutionContext
  
  protected[this] implicit def cf: ClusterFeatures
  
  protected[this] val loadOffsets: Boolean

  // Caches a map of partitions to offsets at a key that is the topic's name.
  private[this] lazy val partitionOffsetsCache: LoadingCache[String, Future[PartitionOffsetsCapture]] = CacheBuilder.newBuilder()
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
    val currentActiveBrokerSet:Set[String] = getBrokerList().list.map(_.host).toSet

    val partitionsByBroker = optPartitionsWithLeaders.map {
      listOfPartAndBroker => listOfPartAndBroker.collect {
        case (part, broker) if broker.isDefined && currentActiveBrokerSet(broker.get.host) => (broker.get, part)
      }.groupBy(_._1)
    }

    def getSimpleConsumer(bi: BrokerIdentity) = {
      require(bi.nonSecure, "Cannot fetch log size without PLAINTEXT endpoint!")
      val port: Int = bi.endpoints(PLAINTEXT)
      new SimpleConsumer(bi.host, port, getSimpleConsumerSocketTimeoutMillis, 256 * 1024, clientId)
    }

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

  protected def getTopicDescription(topic: String, interactive: Boolean) : Option[TopicDescription]

  protected def getBrokerList : () => BrokerList

  protected def readConsumerOffsetByTopicPartition(consumer: String, topic: String, tpi: Map[Int, TopicPartitionIdentity]) : Map[Int, Long]
  
  protected def readConsumerOwnerByTopicPartition(consumer: String, topic: String, tpi: Map[Int, TopicPartitionIdentity]) : Map[Int, String]

  protected def getConsumerTopicsFromIds(consumer: String) : Set[String]

  protected def getConsumerTopicsFromOffsets(consumer: String) : Set[String]

  protected def getConsumerTopicsFromOwners(consumer: String) : Set[String]

  protected def getZKManagedConsumerList: IndexedSeq[ConsumerNameAndType]

  protected def lastUpdateMillisZK : Long

  protected def getConsumerTopics(consumer: String) : Set[String] = {
    getConsumerTopicsFromOffsets(consumer) ++ getConsumerTopicsFromOwners(consumer) ++ getConsumerTopicsFromIds(consumer)
  }

  private[this] var kafkaManagedOffsetCache : Option[KafkaManagedOffsetCache] = None

  private[this] lazy val hasNonSecureEndpoint = getBrokerList().list.exists(_.nonSecure)

  def start() : Unit = {
    if(KafkaManagedOffsetCache.isSupported(clusterContext.config.version)) {
      if(kafkaManagedOffsetCache.isEmpty) {
        info("Starting kafka managed offset cache ...")
        Try {
          val bl = getBrokerList()
          require(bl.list.nonEmpty, "Cannot consume from offset topic when there are no brokers!")
          val of = new KafkaManagedOffsetCache(clusterContext, kafkaAdminClient, consumerProperties, bl)
          kafkaManagedOffsetCache = Option(of)
          val t = new Thread(of, "KafkaManagedOffsetCache")
          t.start()
        }
      }
    }
  }
  
  def stop() : Unit = {
    kafkaManagedOffsetCache.foreach { of =>
      info("Stopping kafka managed offset cache ...")
      Try {
        of.close()
      }
    }
  }

  def getTopicPartitionOffsets(topic: String, interactive: Boolean) : Future[PartitionOffsetsCapture] = {
    if((interactive || loadOffsets) && hasNonSecureEndpoint) {
      partitionOffsetsCache.get(topic)
    } else {
      emptyPartitionOffsetsCapture
    }
  }

  protected def readKafkaManagedConsumerOffsetByTopicPartition(consumer: String
                                                               , topic: String
                                                               , tpi: Map[Int, TopicPartitionIdentity]) : Map[Int, Long] = {
    kafkaManagedOffsetCache.fold(Map.empty[Int,Long]) {
      oc =>
        tpi.map {
          case (part, _) =>
            part -> oc.getOffset(consumer, topic, part).getOrElse(-1L)
        }
    }
  }

  protected def readKafkaManagedConsumerOwnerByTopicPartition(consumer: String
                                                              , topic: String
                                                              , tpi: Map[Int, TopicPartitionIdentity]) : Map[Int, String] = {
    kafkaManagedOffsetCache.fold(Map.empty[Int,String]) {
      oc =>
        tpi.map {
          case (part, _) =>
            part -> oc.getOwner(consumer, topic, part).getOrElse("")
        }
    }
  }

  protected def getKafkaManagedConsumerTopics(consumer: String) : Set[String] = {
    kafkaManagedOffsetCache.fold(Set.empty[String]) {
      oc => oc.getConsumerTopics(consumer)
    }
  }

  protected def getKafkaManagedConsumerList : IndexedSeq[ConsumerNameAndType] = {
    kafkaManagedOffsetCache.fold(IndexedSeq.empty[ConsumerNameAndType]) {
      oc => oc.getConsumers.map(name => ConsumerNameAndType(name, KafkaManagedConsumer))
    }
  }

  final def lastUpdateMillis : Long = {
    Math.max(lastUpdateMillisZK, kafkaManagedOffsetCache.map(_.getLastUpdateTimeMillis).getOrElse(Long.MinValue))
  }

  final def getConsumerDescription(consumer: String, consumerType: ConsumerType) : ConsumerDescription = {
    val consumerTopics: Set[String] = getKafkaVersion match {
      case Kafka_0_8_1_1 => getConsumerTopicsFromOffsets(consumer)
      case _ =>
        consumerType match {
          case ZKManagedConsumer =>
            getConsumerTopicsFromOffsets(consumer) ++ getConsumerTopicsFromOwners(consumer)
          case KafkaManagedConsumer =>
            getKafkaManagedConsumerTopics(consumer)
        }
    }

    val topicDescriptions: Map[String, ConsumedTopicDescription] = consumerTopics.map { topic =>
          val topicDesc = getConsumedTopicDescription(consumer, topic, false, consumerType)
          (topic, topicDesc)
        }.toMap
    ConsumerDescription(consumer, topicDescriptions, consumerType)
  }
  
  final def getConsumedTopicDescription(consumer:String
                                        , topic:String
                                        , interactive: Boolean
                                        , consumerType: ConsumerType) : ConsumedTopicDescription = {
    val optTopic = getTopicDescription(topic, interactive)
    val optTpi = optTopic.map(TopicIdentity.getTopicPartitionIdentity(_, None))
    val (partitionOffsets, partitionOwners) = consumerType match {
      case ZKManagedConsumer =>
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
        (partitionOffsets, partitionOwners)
      case KafkaManagedConsumer =>
        val partitionOffsets = for {
          td <- optTopic
          tpi <- optTpi
        } yield {
          readKafkaManagedConsumerOffsetByTopicPartition(consumer, topic, tpi)
        }
        val partitionOwners = for {
          td <- optTopic
          tpi <- optTpi
        } yield {
          readKafkaManagedConsumerOwnerByTopicPartition(consumer, topic, tpi)
        }
        (partitionOffsets, partitionOwners)
    }

    val numPartitions: Int = math.max(optTopic.flatMap(_.partitionState.map(_.size)).getOrElse(0),
      partitionOffsets.map(_.size).getOrElse(0))
    ConsumedTopicDescription(consumer, topic, numPartitions, optTopic, partitionOwners, partitionOffsets)
  }
  
  final def getConsumerList: ConsumerList = {
    ConsumerList(getKafkaManagedConsumerList ++ getZKManagedConsumerList, clusterContext)
  }
}

case class OffsetCacheActive(curator: CuratorFramework
                             , kafkaAdminClient: KafkaAdminClient
                             , clusterContext: ClusterContext
                             , partitionLeaders: String => Option[List[(Int, Option[BrokerIdentity])]]
                             , topicDescriptions: (String, Boolean) => Option[TopicDescription]
                             , cacheTimeoutSecs: Int
                             , socketTimeoutMillis: Int
                             , kafkaVersion: KafkaVersion
                             , consumerProperties: Option[Properties]
                             , getBrokerList : () => BrokerList
                              )
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

  protected def getTopicDescription(topic: String, interactive: Boolean) : Option[TopicDescription] = topicDescriptions(topic, interactive)
  
  override def start():  Unit = {
    super.start()
    info("Starting consumers tree cache...")
    consumersTreeCache.start()

    info("Adding consumers tree cache listener...")
    consumersTreeCache.getListenable.addListener(consumersTreeCacheListener)
  }
  
  override def stop(): Unit = {
    super.stop()
    info("Removing consumers tree cache listener...")
    Try(consumersTreeCache.getListenable.removeListener(consumersTreeCacheListener))
    
    info("Shutting down consumers tree cache...")
    Try(consumersTreeCache.close())
  }

  protected def lastUpdateMillisZK : Long = consumersTreeCacheLastUpdateMillis

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

  protected def getZKManagedConsumerList: IndexedSeq[ConsumerNameAndType] = {
    withConsumersTreeCache { cache =>
      cache.getCurrentChildren(ZkUtils.ConsumersPath)
    }.fold {
      IndexedSeq.empty[ConsumerNameAndType]
    } { data: java.util.Map[String, ChildData] =>
      data.asScala.filter{
        case (consumer, childData) =>
          if (clusterContext.config.filterConsumers)
          // Defining "inactive consumer" as a consumer that is missing one of three children ids/ offsets/ or owners/
            childData.getStat.getNumChildren > 2
          else true
      }.keySet.toIndexedSeq.map(name => ConsumerNameAndType(name, ZKManagedConsumer))
    }
  }
}

case class OffsetCachePassive(curator: CuratorFramework
                              , kafkaAdminClient: KafkaAdminClient
                              , clusterContext: ClusterContext
                              , partitionLeaders: String => Option[List[(Int, Option[BrokerIdentity])]]
                              , topicDescriptions: (String, Boolean) => Option[TopicDescription]
                              , cacheTimeoutSecs: Int
                              , socketTimeoutMillis: Int
                              , kafkaVersion: KafkaVersion
                              , consumerProperties: Option[Properties]
                              , getBrokerList : () => BrokerList
                               )
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

  protected def getTopicDescription(topic: String, interactive: Boolean) : Option[TopicDescription] = topicDescriptions(topic, interactive)

  override def start():  Unit = {
    super.start()
    info("Starting consumers path children cache...")
    consumersPathChildrenCache.start(StartMode.BUILD_INITIAL_CACHE)

    info("Adding consumers path children cache listener...")
    consumersPathChildrenCache.getListenable.addListener(consumersPathChildrenCacheListener)
  }

  override def stop(): Unit = {
    super.stop()
    info("Removing consumers path children cache listener...")
    Try(consumersPathChildrenCache.getListenable.removeListener(consumersPathChildrenCacheListener))

    info("Shutting down consumers path children cache...")
    Try(consumersPathChildrenCache.close())
  }

  protected def lastUpdateMillisZK : Long = consumersTreeCacheLastUpdateMillis

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

  protected def getZKManagedConsumerList: IndexedSeq[ConsumerNameAndType] = {
    withConsumersPathChildrenCache { cache =>
      val currentData = cache.getCurrentData
      currentData
    }.fold {
      IndexedSeq.empty[ConsumerNameAndType]
    } { data: java.util.List[ChildData] =>
      data.asScala.map(cd => ConsumerNameAndType(cd.getPath.split("/").last, ZKManagedConsumer)).toIndexedSeq
    }
  }
}

case class KafkaStateActorConfig(curator: CuratorFramework
                                 , pinnedDispatcherName: String
                                 , clusterContext: ClusterContext
                                 , offsetCachePoolConfig: LongRunningPoolConfig
                                 , kafkaAdminClientPoolConfig: LongRunningPoolConfig
                                 , partitionOffsetCacheTimeoutSecs: Int
                                 , simpleConsumerSocketTimeoutMillis: Int
                                 , consumerProperties: Option[Properties]
                                  )
class KafkaStateActor(config: KafkaStateActorConfig) extends BaseClusterQueryCommandActor with LongRunningPoolActor {

  protected implicit val clusterContext: ClusterContext = config.clusterContext

  protected implicit val cf: ClusterFeatures = clusterContext.clusterFeatures

  override protected def longRunningPoolConfig: LongRunningPoolConfig = config.offsetCachePoolConfig

  override protected def longRunningQueueFull(): Unit = {
    log.error("Long running pool queue full, skipping!")
  }

  private[this] val kaConfig = KafkaAdminClientActorConfig(
    clusterContext,
    config.kafkaAdminClientPoolConfig,
    self.path,
    config.consumerProperties
  )
  private[this] val kaProps = Props(classOf[KafkaAdminClientActor],kaConfig)
  private[this] val kafkaAdminClientActor : ActorPath = context.actorOf(kaProps.withDispatcher(config.pinnedDispatcherName),"kafka-admin-client").path
  private[this] val kafkaAdminClient = new KafkaAdminClient(context, kafkaAdminClientActor)

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
  
  private[this] lazy val offsetCache: OffsetCache = {
    if(config.clusterContext.config.activeOffsetCacheEnabled)
      new OffsetCacheActive(config.curator
        , kafkaAdminClient
        , config.clusterContext
        , getPartitionLeaders
        , getTopicDescription
        , config.partitionOffsetCacheTimeoutSecs
        , config.simpleConsumerSocketTimeoutMillis
        , config.clusterContext.config.version
        , config.consumerProperties
        , () => getBrokerList
      )(longRunningExecutionContext, cf)
    else
      new OffsetCachePassive( config.curator
        , kafkaAdminClient
        , config.clusterContext
        , getPartitionLeaders
        , getTopicDescription
        , config.partitionOffsetCacheTimeoutSecs
        , config .simpleConsumerSocketTimeoutMillis
        , config.clusterContext.config.version
        , config.consumerProperties
        , () => getBrokerList
      )(longRunningExecutionContext, cf)
  }

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
          offsetCache.getConsumerList
        }

      case KSGetTopicConfig(topic) =>
        sender ! TopicConfig(topic, getTopicConfigString(topic))

      case KSGetTopicDescription(topic) =>
        sender ! getTopicDescription(topic, false)

      case KSGetTopicDescriptions(topics) =>
        sender ! TopicDescriptions(topics.toIndexedSeq.flatMap(getTopicDescription(_, false)), topicsTreeCacheLastUpdateMillis)

      case KSGetConsumerDescription(consumer, consumerType) =>
        asyncPipeToSender {
          offsetCache.getConsumerDescription(consumer, consumerType)
        }

      case KSGetConsumedTopicDescription(consumer, topic, consumerType) =>
        asyncPipeToSender {
          offsetCache.getConsumedTopicDescription(consumer, topic, true, consumerType)
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
            ConsumerDescriptions(offsetCache
              .getConsumerList
              .list
              .map(c => offsetCache.getConsumerDescription(c.name, c.consumerType)), offsetCache.lastUpdateMillis)
          }
        }

      case KSGetTopicsLastUpdateMillis =>
        sender ! topicsTreeCacheLastUpdateMillis

      case KSGetBrokers =>
        sender ! getBrokerList

      case KSGetPreferredLeaderElection =>
        sender ! preferredLeaderElection

      case KSGetReassignPartition =>
        sender ! reassignPartitions

      case any: Any => log.warning("ksa : processQueryRequest : Received unknown message: {}", any.toString)
    }
  }

  private def getBrokerList : BrokerList = {
    BrokerList(getBrokers, config.clusterContext)
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

}

