/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import akka.pattern._
import kafka.manager.features.KMDeleteTopicFeature
import java.util.concurrent.TimeUnit

import com.google.common.cache.{LoadingCache, CacheLoader, CacheBuilder}
import kafka.api.{PartitionOffsetRequestInfo, OffsetRequest}
import kafka.consumer.SimpleConsumer
import kafka.common.TopicAndPartition
import kafka.manager.utils.zero81.{ReassignPartitionCommand, PreferredReplicaLeaderElectionCommand}
import kafka.manager.utils.ZkUtils
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.cache._
import org.apache.curator.framework.CuratorFramework
import org.joda.time.{DateTimeZone, DateTime}
import org.slf4j.{LoggerFactory, Logger}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure, Try}

/**
 * @author hiral
 */
import ActorModel._
import kafka.manager.utils._
import scala.collection.JavaConverters._

trait OffsetCache {
  
  def getCacheTimeoutSecs: Int

  def getSimpleConsumerSocketTimeoutMillis: Int

  protected[this] implicit def ec: ExecutionContext
  
  protected[this] lazy val log : Logger = LoggerFactory.getLogger(this.getClass)

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
                log.error(s"[topic=$topic] An error has occurred while getting topic offsets from broker $parts", t)
                Map.empty[Int, Option[Long]]
              }
          }
          val result: Future[Map[Int, Option[Long]]] = Future.sequence(listOfFutures).map(_.foldRight(Map.empty[Int, Option[Long]])((b, a) => b ++ a))
          result.map(m => PartitionOffsetsCapture(System.currentTimeMillis(), m.mapValues(_.getOrElse(0L))))
        } 
        catch {
          case e: Exception =>
            log.error(s"Failed to get offsets for topic $topic", e)
            Future.failed(e)
        }
      }
    }

    futureMap onFailure {
      case t => log.error(s"[topic=$topic] An error has occurred while getting topic offsets", t)
    }
    futureMap
  }
  
  protected def getTopicPartitionLeaders(topic: String) : Option[List[(Int, Option[BrokerIdentity])]]

  protected def getTopicDescription(topic: String) : Option[TopicDescription]

  def start()
  
  def stop()

  def getTopicPartitionOffsets(topic: String) : Future[PartitionOffsetsCapture] = partitionOffsetsCache.get(topic)
  
  def lastUpdateMillis : Long
  
  def getConsumerDescription(consumer: String) : Option[ConsumerDescription]
  
  def getConsumedTopicDescription(consumer:String, topic:String) : ConsumedTopicDescription
  
  def getConsumerList: ConsumerList
}

case class OffsetCacheActive(curator: CuratorFramework,
                                  clusterContext: ClusterContext, 
                                  partitionLeaders: String => Option[List[(Int, Option[BrokerIdentity])]],
                                  topicDescriptions: String => Option[TopicDescription],
                                  cacheTimeoutSecs: Int,
                                  socketTimeoutMillis: Int)
                                 (implicit protected[this] val ec: ExecutionContext) extends OffsetCache {

  def getCacheTimeoutSecs: Int = cacheTimeoutSecs

  def getSimpleConsumerSocketTimeoutMillis: Int = socketTimeoutMillis

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

  protected def getTopicDescription(topic: String) : Option[TopicDescription] = topicDescriptions(topic)
  
  def start():  Unit = {
    log.info("Starting consumers tree cache...")
    consumersTreeCache.start()

    log.info("Adding consumers tree cache listener...")
    consumersTreeCache.getListenable.addListener(consumersTreeCacheListener)
  }
  
  def stop(): Unit = {
    log.info("Removing consumers tree cache listener...")
    Try(consumersTreeCache.getListenable.removeListener(consumersTreeCacheListener))
    
    log.info("Shutting down consumers tree cache...")
    Try(consumersTreeCache.close())
  }

  def lastUpdateMillis : Long = consumersTreeCacheLastUpdateMillis

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
  }

  def getConsumedTopicDescription(consumer:String, topic:String) : ConsumedTopicDescription = {
    val offsetPath = "%s/%s/%s/%s".format(ZkUtils.ConsumersPath, consumer, "offsets", topic)
    val ownerPath = "%s/%s/%s/%s".format(ZkUtils.ConsumersPath, consumer, "owners", topic)
    val partitionOffsets: Option[Map[Int, Long]] = for {
      offsetsByPartition: Map[String, ChildData] <- Option(consumersTreeCache.getCurrentChildren(offsetPath)).map(_.asScala.toMap)
      offsets : Map[Int, Long] = offsetsByPartition map {case (part, data) => (part.toInt, Option(data.getData).map(asString).getOrElse("-1").toLong)}
    } yield offsets

    val partitionOwners: Option[Map[Int, String]] = for {
      ownersByPartition: Map[String, ChildData] <- Option(consumersTreeCache.getCurrentChildren(ownerPath)).map(_.asScala.toMap)
      owners : Map[Int, String] = ownersByPartition map { case (part, data) => (part.toInt, Option(data.getData).map(asString).getOrElse("")) }
    } yield owners

    val optTopic = getTopicDescription(topic)
    val numPartitions: Int = math.max(optTopic.flatMap(_.partitionState.map(_.size)).getOrElse(0),
      partitionOffsets.map(_.size).getOrElse(0))
    ConsumedTopicDescription(consumer, topic, numPartitions, optTopic, partitionOwners, partitionOffsets)
  }

  def getConsumerList: ConsumerList = {
    withConsumersTreeCache { cache =>
      cache.getCurrentChildren(ZkUtils.ConsumersPath)
    }.fold {
      ConsumerList(IndexedSeq.empty, clusterContext)
    } { data: java.util.Map[String, ChildData] =>
      val filteredList: IndexedSeq[String] = data.asScala.filter{
        case (consumer, childData) =>
          if (clusterContext.config.filterConsumers)
          // Defining "inactive consumer" as a consumer that is missing one of three children ids/ offsets/ or owners/
            childData.getStat.getNumChildren > 2
          else true
      }.keySet.toIndexedSeq
      ConsumerList(filteredList, clusterContext)
    }
  }
}

case class OffsetCachePassive(curator: CuratorFramework,
                             clusterContext: ClusterContext,
                             partitionLeaders: String => Option[List[(Int, Option[BrokerIdentity])]],
                             topicDescriptions: String => Option[TopicDescription],
                             cacheTimeoutSecs: Int,
                             socketTimeoutMillis: Int)
                            (implicit protected[this] val ec: ExecutionContext) extends OffsetCache {

  def getCacheTimeoutSecs: Int = cacheTimeoutSecs

  def getSimpleConsumerSocketTimeoutMillis: Int = socketTimeoutMillis

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

  protected def getTopicDescription(topic: String) : Option[TopicDescription] = topicDescriptions(topic)

  def start():  Unit = {
    log.info("Starting consumers path children cache...")
    consumersPathChildrenCache.start(StartMode.BUILD_INITIAL_CACHE)

    log.info("Adding consumers path children cache listener...")
    consumersPathChildrenCache.getListenable.addListener(consumersPathChildrenCacheListener)
  }

  def stop(): Unit = {
    log.info("Removing consumers path children cache listener...")
    Try(consumersPathChildrenCache.getListenable.removeListener(consumersPathChildrenCacheListener))

    log.info("Shutting down consumers path children cache...")
    Try(consumersPathChildrenCache.close())
  }

  def lastUpdateMillis : Long = consumersTreeCacheLastUpdateMillis

  def getConsumerDescription(consumer: String) : Option[ConsumerDescription] = {
    val offsetPath = "%s/%s/%s".format(ZkUtils.ConsumersPath,consumer,"offsets")
    val topicOffsetOption : Option[List[String]] = Try(Option(curator.getChildren.forPath(offsetPath)).map(_.asScala.toList)).toOption.flatten
    
    val topicDescriptions: Option[Map[String, ConsumedTopicDescription]] = topicOffsetOption.map {
      topics =>
        topics.map { topic =>
          val topicDesc = getConsumedTopicDescription(consumer, topic)
          (topic, topicDesc)
        }.toMap
    }

    topicDescriptions.map(ConsumerDescription(consumer, _))
  }
  
  private[this] def readConsumerOffsetByTopicPartition(consumer: String, topic: String, tpi: Map[Int, TopicPartitionIdentity]) : Map[Int, Long] = {
    tpi.map {
      case (p, _) =>
        val offsetPath = "%s/%s/%s/%s/%s".format(ZkUtils.ConsumersPath, consumer, "offsets", topic, p)
        (p, ZkUtils.readDataMaybeNull(curator, offsetPath)._1.map(_.toLong).getOrElse(-1L))
    }
  }

  private[this] def readConsumerOwnerByTopicPartition(consumer: String, topic: String, tpi: Map[Int, TopicPartitionIdentity]) : Map[Int, String] = {
    tpi.map {
      case (p, _) =>
        val ownerPath = "%s/%s/%s/%s/%s".format(ZkUtils.ConsumersPath, consumer, "owners", topic, p)
        (p, ZkUtils.readDataMaybeNull(curator, ownerPath)._1.orNull)
    }.filter(_._2 != null)
  }
  
  def getConsumedTopicDescription(consumer:String, topic:String) : ConsumedTopicDescription = {
    val optTopic = getTopicDescription(topic)
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

  def getConsumerList: ConsumerList = {
    withConsumersPathChildrenCache { cache =>
      val currentData = cache.getCurrentData
      currentData
    }.fold {
      ConsumerList(IndexedSeq.empty, clusterContext)
    } { data: java.util.List[ChildData] =>
      ConsumerList(data.asScala.map(cd => cd.getPath.split("/").last).toIndexedSeq, clusterContext)
    }
  }
}

case class KafkaStateActorConfig(curator: CuratorFramework,
                                 clusterContext: ClusterContext,
                                 longRunningPoolConfig: LongRunningPoolConfig,
                                 partitionOffsetCacheTimeoutSecs: Int, simpleConsumerSocketTimeoutMillis: Int)
class KafkaStateActor(config: KafkaStateActorConfig) extends BaseQueryCommandActor with LongRunningPoolActor {

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
  
  private[this] val offsetCache: OffsetCache = {
    if(config.clusterContext.config.activeOffsetCacheEnabled)
      new OffsetCacheActive(
        config.curator,
        config.clusterContext,
        getPartitionLeaders,
        getTopicDescription,
        config.partitionOffsetCacheTimeoutSecs,
        config.simpleConsumerSocketTimeoutMillis)(longRunningExecutionContext)
    else
      new OffsetCachePassive(
        config.curator,
        config.clusterContext,
        getPartitionLeaders,
        getTopicDescription,
        config.partitionOffsetCacheTimeoutSecs,
        config .simpleConsumerSocketTimeoutMillis)(longRunningExecutionContext)
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

  def getTopicDescription(topic: String) : Option[TopicDescription] = {
    for {
      description <- getTopicZookeeperData(topic)
      partitionsPath = "%s/%s/partitions".format(ZkUtils.BrokerTopicsPath, topic)
      partitions: Map[String, ChildData] <- Option(topicsTreeCache.getCurrentChildren(partitionsPath)).map(_.asScala.toMap)
      states : Map[String, String] = partitions flatMap { case (part, _) =>
        val statePath = s"$partitionsPath/$part/state"
        Option(topicsTreeCache.getCurrentData(statePath)).map(cd => (part, asString(cd.getData)))
      }
      partitionOffsets = offsetCache.getTopicPartitionOffsets(topic)
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
        val deleteSet: Set[String] = {
          if(config.clusterContext.clusterFeatures.features(KMDeleteTopicFeature)) {
            val deleteTopicsData: mutable.Buffer[ChildData] = deleteTopicsPathCache.getCurrentData.asScala
            deleteTopicsData.map { cd =>
              nodeFromPath(cd.getPath)
            }.toSet
          } else {
            Set.empty
          }
        }
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
        sender ! getTopicDescription(topic)

      case KSGetTopicDescriptions(topics) =>
        sender ! TopicDescriptions(topics.toIndexedSeq.flatMap(getTopicDescription), topicsTreeCacheLastUpdateMillis)

      case KSGetConsumerDescription(consumer) =>
        asyncPipeToSender {
          offsetCache.getConsumerDescription(consumer)
        }

      case KSGetConsumedTopicDescription(consumer, topic) =>
        asyncPipeToSender {
          offsetCache.getConsumedTopicDescription(consumer, topic)
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
            sender ! TopicDescriptions(data.asScala.keys.toIndexedSeq.flatMap(getTopicDescription), topicsTreeCacheLastUpdateMillis)
          }
        } // else no updates to send

      case KSGetAllConsumerDescriptions(lastUpdateMillisOption) =>
        val lastUpdateMillis = lastUpdateMillisOption.getOrElse(0L)
        if (offsetCache.lastUpdateMillis > lastUpdateMillis) {
          asyncPipeToSender {
            ConsumerDescriptions(offsetCache
              .getConsumerList
              .list
              .flatMap(c => offsetCache.getConsumerDescription(c)), offsetCache.lastUpdateMillis)
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

}

