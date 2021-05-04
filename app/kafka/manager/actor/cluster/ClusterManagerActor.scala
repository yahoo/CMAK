/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.actor.cluster

import akka.actor.{ActorPath, Props}
import akka.pattern._
import akka.util.Timeout
import kafka.manager.base._
import kafka.manager.base.cluster.BaseClusterQueryCommandActor
import kafka.manager.features.{ClusterFeatures, KMJMXMetricsFeature, KMLogKafkaFeature}
import kafka.manager.logkafka._
import kafka.manager.model.{ClusterConfig, ClusterContext, CuratorConfig}
import kafka.manager.utils.AdminUtils
import kafka.manager.utils.zero81.SchedulePreferredLeaderElectionCommand
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.kafka.common.TopicPartition
import org.apache.zookeeper.CreateMode

import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Try}

/**
 * @author hiral
 */
object ClusterManagerActor {
  import org.json4s._
  import org.json4s.jackson.Serialization.{read, write}
  implicit val formats = DefaultFormats

  def serializeAssignments(assignments: Map[Int, Seq[Int]]) : Array[Byte] = {
    write(assignments).getBytes(StandardCharsets.UTF_8)
  }

  def deserializeAssignments(ba: Array[Byte]) : Map[Int, Seq[Int]] = {
    val json = new String(ba, StandardCharsets.UTF_8)
    read[Map[Int,Seq[Int]]](json)
  }
}

import kafka.manager.model.ActorModel._

case class ClusterManagerActorConfig(pinnedDispatcherName: String
                                     , baseZkPath : String
                                     , curatorConfig: CuratorConfig
                                     , clusterConfig: ClusterConfig
                                     , consumerProperties: Option[Properties]
                                     , askTimeoutMillis: Long = 2000
                                     , mutexTimeoutMillis: Int = 4000
                                     , simpleConsumerSocketTimeoutMillis: Int = 10000
                                    )

class ClusterManagerActor(cmConfig: ClusterManagerActorConfig)
  extends BaseClusterQueryCommandActor with CuratorAwareActor with BaseZkPath {

  require(cmConfig.clusterConfig.tuning.isDefined, s"No tuning defined : ${cmConfig.clusterConfig}")
  import ClusterManagerActor._
  protected implicit val clusterContext: ClusterContext = ClusterContext(ClusterFeatures.from(cmConfig.clusterConfig), cmConfig.clusterConfig)

  //this is from base zk path trait
  override def baseZkPath : String = cmConfig.baseZkPath

  //this is for curator aware actor
  override def curatorConfig: CuratorConfig = cmConfig.curatorConfig

  val longRunningExecutor = new ThreadPoolExecutor(
    clusterConfig.tuning.get.clusterManagerThreadPoolSize.get
    , clusterConfig.tuning.get.clusterManagerThreadPoolSize.get
    ,0L
    ,TimeUnit.MILLISECONDS
    ,new LinkedBlockingQueue[Runnable](clusterConfig.tuning.get.clusterManagerThreadPoolQueueSize.get)
  )

  val longRunningExecutionContext = ExecutionContext.fromExecutor(longRunningExecutor)

  protected[this] val sharedClusterCurator : CuratorFramework = getCurator(cmConfig.clusterConfig.curatorConfig)
  log.info("Starting shared curator...")
  sharedClusterCurator.start()

  //create cluster path
  Try(curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(baseZkPath))
  require(curator.checkExists().forPath(baseZkPath) != null,s"Cluster path not found : $baseZkPath")

  private[this] val baseTopicsZkPath = zkPath("topics")

  //create cluster path for storing topics state
  Try(curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(baseTopicsZkPath))
  require(curator.checkExists().forPath(baseTopicsZkPath) != null,s"Cluster path for topics not found : $baseTopicsZkPath")

  private[this] val mutex = new InterProcessSemaphoreMutex(curator, zkPath("mutex"))

  private[this] val adminUtils = new AdminUtils(cmConfig.clusterConfig.version)

  private[this] val ksConfig = {
    val kafkaManagedOffsetCacheConfigOption : Option[KafkaManagedOffsetCacheConfig] = for {
      tuning <- cmConfig.clusterConfig.tuning
      groupMemberMetadataCheckMillis = tuning.kafkaManagedOffsetMetadataCheckMillis
      groupTopicPartitionOffsetExpireDays =  tuning.kafkaManagedOffsetGroupExpireDays
      groupTopicPartitionOffsetMaxSize = tuning.kafkaManagedOffsetGroupCacheSize
    } yield {
      KafkaManagedOffsetCacheConfig(
        groupMemberMetadataCheckMillis = groupMemberMetadataCheckMillis.getOrElse(KafkaManagedOffsetCacheConfig.defaultGroupMemberMetadataCheckMillis)
        , groupTopicPartitionOffsetExpireDays = groupTopicPartitionOffsetExpireDays.getOrElse(KafkaManagedOffsetCacheConfig.defaultGroupTopicPartitionOffsetExpireDays)
        , groupTopicPartitionOffsetMaxSize = groupTopicPartitionOffsetMaxSize.getOrElse(KafkaManagedOffsetCacheConfig.defaultGroupTopicPartitionOffsetMaxSize)
      )
    }
    KafkaStateActorConfig(
      sharedClusterCurator
      , cmConfig.pinnedDispatcherName
      , clusterContext
      , LongRunningPoolConfig(clusterConfig.tuning.get.offsetCacheThreadPoolSize.get, clusterConfig.tuning.get.offsetCacheThreadPoolQueueSize.get)
      , LongRunningPoolConfig(clusterConfig.tuning.get.kafkaAdminClientThreadPoolSize.get, clusterConfig.tuning.get.kafkaAdminClientThreadPoolQueueSize.get)
      , clusterConfig.tuning.get.partitionOffsetCacheTimeoutSecs.get
      , cmConfig.simpleConsumerSocketTimeoutMillis
      , cmConfig.consumerProperties
      , kafkaManagedOffsetCacheConfig = kafkaManagedOffsetCacheConfigOption.getOrElse(KafkaManagedOffsetCacheConfig()
      )
    )
  }
  private[this] val ksProps = Props(classOf[KafkaStateActor],ksConfig)
  private[this] val kafkaStateActor : ActorPath = context.actorOf(ksProps.withDispatcher(cmConfig.pinnedDispatcherName),"kafka-state").path

  private[this] val bvConfig = BrokerViewCacheActorConfig(
    kafkaStateActor
    , clusterContext
    , LongRunningPoolConfig(clusterConfig.tuning.get.brokerViewThreadPoolSize.get, clusterConfig.tuning.get.brokerViewThreadPoolQueueSize.get)
    , FiniteDuration(clusterConfig.tuning.get.brokerViewUpdatePeriodSeconds.get, TimeUnit.SECONDS))
  private[this] val bvcProps = Props(classOf[BrokerViewCacheActor],bvConfig)
  private[this] val brokerViewCacheActor : ActorPath = context.actorOf(bvcProps,"broker-view").path

  private[this] val kcProps = {
    val kcaConfig = KafkaCommandActorConfig(
      sharedClusterCurator
      , LongRunningPoolConfig(clusterConfig.tuning.get.kafkaCommandThreadPoolSize.get, clusterConfig.tuning.get.kafkaCommandThreadPoolQueueSize.get)
      , cmConfig.askTimeoutMillis
      , clusterContext
      , adminUtils)
    Props(classOf[KafkaCommandActor],kcaConfig)
  }
  private[this] val kafkaCommandActor : ActorPath = context.actorOf(kcProps,"kafka-command").path

  private[this] val lksProps: Option[Props] =
    featureGateFold(KMLogKafkaFeature)(
      None,
      Some(Props(classOf[LogkafkaStateActor],sharedClusterCurator, clusterContext))
    )
  
  private[this] val logkafkaStateActor : Option[ActorPath] =
    featureGateFold(KMLogKafkaFeature)(
      None,
      Some(context.actorOf(lksProps.get.withDispatcher(cmConfig.pinnedDispatcherName),"logkafka-state").path)
    )

  private[this] val lkvConfig: Option[LogkafkaViewCacheActorConfig] =
    featureGateFold(KMLogKafkaFeature)(
      None,
      Some(
        LogkafkaViewCacheActorConfig(
          logkafkaStateActor.get,
          clusterContext,
          LongRunningPoolConfig(Runtime.getRuntime.availableProcessors(), 1000),
          FiniteDuration(clusterConfig.tuning.get.logkafkaUpdatePeriodSeconds.get, TimeUnit.SECONDS)
        )
      )
    )

  private[this] val lkvcProps: Option[Props] =
    featureGateFold(KMLogKafkaFeature)(
      None,
      Some(Props(classOf[LogkafkaViewCacheActor],lkvConfig.get))
    )

  private[this] val logkafkaViewCacheActor: Option[ActorPath] =
    featureGateFold(KMLogKafkaFeature)(
      None,
      Some(context.actorOf(lkvcProps.get,"logkafka-view").path)
    )

  private[this] val lkcProps: Option[Props] =
    featureGateFold(KMLogKafkaFeature)(
      None,
      Some(
        Props(classOf[LogkafkaCommandActor],
          LogkafkaCommandActorConfig(
            sharedClusterCurator
            , LongRunningPoolConfig(clusterConfig.tuning.get.logkafkaCommandThreadPoolSize.get, clusterConfig.tuning.get.logkafkaCommandThreadPoolQueueSize.get)
            , cmConfig.askTimeoutMillis
            , clusterContext)
        )
      )
    )

  private[this] val logkafkaCommandActor : Option[ActorPath] =
    featureGateFold(KMLogKafkaFeature)(
      None,
      Some(context.actorOf(lkcProps.get,"logkafka-command").path)
    )

  private[this] implicit val timeout: Timeout = FiniteDuration(cmConfig.askTimeoutMillis,MILLISECONDS)

  private[this] val clusterManagerTopicsPathCache = new PathChildrenCache(curator,baseTopicsZkPath,true)

  @scala.throws[Exception](classOf[Exception])
  override def preStart() = {
    super.preStart()
    log.info("Started actor %s".format(self.path))
    log.info("Starting cluster manager topics path cache...")
    clusterManagerTopicsPathCache.start(StartMode.BUILD_INITIAL_CACHE)
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
    log.info("Shutting down shared curator...")
    Try(sharedClusterCurator.close())

    log.info("Shutting down cluster manager topics path cache...")
    Try(clusterManagerTopicsPathCache.close())
    super.postStop()
  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case any: Any => log.warning("cma : processActorResponse : Received unknown message: {}", any)
    }
  }

  override def processQueryRequest(request: QueryRequest): Unit = {
    request match {
      case ksRequest: KSRequest =>
        context.actorSelection(kafkaStateActor).forward(ksRequest)

      case lksRequest: LKSRequest =>
        logkafkaStateActor.isDefined match {
          case true => context.actorSelection(logkafkaStateActor.get).forward(lksRequest)
          case false => log.warning("cma: processQueryResponse : Received LKSRequest", lksRequest)
        }

      case bvRequest: BVRequest =>
        context.actorSelection(brokerViewCacheActor).forward(bvRequest)

      case lkvRequest: LKVRequest =>
        logkafkaStateActor.isDefined match {
          case true => context.actorSelection(logkafkaViewCacheActor.get).forward(lkvRequest)
          case false =>  log.warning("cma: processQueryResponse : Received LKVRequest", lkvRequest)
        }

      case CMGetClusterContext =>
        sender ! clusterContext
        
      case CMGetView =>
        implicit val ec = context.dispatcher
        val eventualBrokerList = withKafkaStateActor(KSGetBrokers)(identity[BrokerList])
        val eventualTopicList = withKafkaStateActor(KSGetTopics)(identity[TopicList])
        val result = for {
          bl <- eventualBrokerList
          tl <- eventualTopicList
        } yield CMView(tl.list.size, bl.list.size, clusterContext)
        result pipeTo sender

      case CMGetTopicIdentity(topic) =>
        implicit val ec = context.dispatcher
        val eventualBrokerList = withKafkaStateActor(KSGetBrokers)(identity[BrokerList])
        val eventualTopicMetrics : Future[Option[BrokerMetrics]] = {
          featureGateFold(KMJMXMetricsFeature)(
            Future.successful(None),
            withBrokerViewCacheActor(BVGetTopicMetrics(topic))(identity[Option[BrokerMetrics]])
          )
        }
        val eventualTopicDescription = withKafkaStateActor(KSGetTopicDescription(topic))(identity[Option[TopicDescription]])
        val eventualTopicPartitionSizes = withBrokerViewCacheActor(BVGetBrokerTopicPartitionSizes(topic))(identity[Option[Map[Int, Map[Int, Long]]]])
        val result: Future[Option[CMTopicIdentity]] = for {
          bl <- eventualBrokerList
          tm <- eventualTopicMetrics
          tdO <- eventualTopicDescription
          tp <- eventualTopicPartitionSizes
        } yield tdO.map( td => CMTopicIdentity(Try(TopicIdentity.from(bl,td,tm,tp,clusterContext,None))))
        result pipeTo sender

      case CMGetLogkafkaIdentity(logkafka_id) =>
        implicit val ec = context.dispatcher
        val eventualLogkafkaConfig= withLogkafkaStateActor(LKSGetLogkafkaConfig(logkafka_id))(identity[Option[LogkafkaConfig]])
        val eventualLogkafkaClient= withLogkafkaStateActor(LKSGetLogkafkaClient(logkafka_id))(identity[Option[LogkafkaClient]])
        val result: Future[Option[CMLogkafkaIdentity]] = for {
          lcg <- eventualLogkafkaConfig
          lct <- eventualLogkafkaClient
        } yield Some(CMLogkafkaIdentity(Try(LogkafkaIdentity.from(logkafka_id,lcg,lct))))
        result pipeTo sender

      case CMGetConsumerIdentity(consumer, consumerType) =>
        implicit val ec = context.dispatcher
        val eventualConsumerDescription = withKafkaStateActor(KSGetConsumerDescription(consumer, consumerType))(identity[ConsumerDescription])
        val result: Future[CMConsumerIdentity] = for {
          cd <- eventualConsumerDescription
          ciO = CMConsumerIdentity(Try(ConsumerIdentity.from(cd,clusterContext)))
        } yield ciO
        result pipeTo sender

      case CMGetConsumedTopicState(consumer, topic, consumerType) =>
        implicit val ec = context.dispatcher
        val eventualConsumedTopicDescription = withKafkaStateActor(
          KSGetConsumedTopicDescription(consumer,topic, consumerType)
        )(identity[ConsumedTopicDescription])
        val result: Future[CMConsumedTopic] = eventualConsumedTopicDescription.map{
          ctd: ConsumedTopicDescription =>  CMConsumedTopic(Try(ConsumedTopicState.from(ctd, clusterContext)))
        }
        result pipeTo sender

      case CMGetGeneratedPartitionAssignments(topic) =>
        implicit val ec = context.dispatcher
        val eventualBrokerList = withKafkaStateActor(KSGetBrokers)(identity[BrokerList])
        val topicIdentityAndNonExistingBrokers = for {
          bl <- eventualBrokerList
          assignments = getGeneratedPartitionAssignments(topic)
          nonExistentBrokers = getNonExistentBrokers(bl, assignments)
        } yield GeneratedPartitionAssignments(topic, assignments, nonExistentBrokers)
        topicIdentityAndNonExistingBrokers pipeTo sender

      case any: Any => log.warning("cma : processQueryResponse : Received unknown message: {}", any)
    }
  }

  private def updateAssignmentInZk(topic: String, assignment: Map[Int, Seq[Int]]) = {
    implicit val ec = longRunningExecutionContext

    Try {
      val topicZkPath = zkPathFrom(baseTopicsZkPath, topic)
      val data = serializeAssignments(assignment)
      Option(clusterManagerTopicsPathCache.getCurrentData(topicZkPath)).fold[Unit] {
        log.info(s"Creating and saving generated data $topicZkPath")
        curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(topicZkPath, data)
      } { _ =>
        log.info(s"Updating generated data $topicZkPath")
        curator.setData().forPath(topicZkPath, data)
      }
    }
  }

  private def writeScheduleLeaderElectionToZk(schedule: Map[String, Int]) = {
    implicit val ec = longRunningExecutionContext

    log.info("Updating schedule for preferred leader election")
    SchedulePreferredLeaderElectionCommand.writeScheduleLeaderElectionData(curator, schedule)
  }
  
  implicit private def toTryClusterContext(t: Try[Unit]) : Try[ClusterContext] = {
    t.map(_ => clusterContext)
  }
  
  private[this] def getGeneratedPartitionAssignments(topic: String) : Map[Int, Seq[Int]] = {
    val topicZkPath = zkPathFrom(baseTopicsZkPath, topic)
    Option(clusterManagerTopicsPathCache.getCurrentData(topicZkPath)).fold {
      throw new IllegalArgumentException(s"No generated assignment found for topic $topic")
    } { childData =>
      deserializeAssignments(childData.getData)
    }
  }

  override def processCommandRequest(request: CommandRequest): Unit = {
    request match {
      case CMShutdown =>
        log.info(s"Shutting down cluster manager ${cmConfig.clusterConfig.name}")
        context.children.foreach(context.stop)
        shutdown = true

      case CMDeleteTopic(topic) =>
        implicit val ec = longRunningExecutionContext
        withKafkaCommandActor(KCDeleteTopic(topic)) {
          kcResponse: KCCommandResult =>
            Future.successful(CMCommandResult(kcResponse.result))
        } pipeTo sender()
        
      case CMCreateTopic(topic, partitions, replication, config) =>
        implicit val ec = longRunningExecutionContext
        val eventualTopicDescription = withKafkaStateActor(KSGetTopicDescription(topic))(identity[Option[TopicDescription]])
        val eventualBrokerList = withKafkaStateActor(KSGetBrokers)(identity[BrokerList])
        eventualTopicDescription.map { topicDescriptionOption =>
          topicDescriptionOption.fold {
            eventualBrokerList.flatMap {
              bl => withKafkaCommandActor(KCCreateTopic(topic, bl.list.map(_.id).toSet, partitions, replication, config)) {
                kcResponse: KCCommandResult =>
                  CMCommandResult(kcResponse.result)
              }
            }
          } { td =>
            Future.successful(CMCommandResult(Failure(new IllegalArgumentException(s"Topic already exists : $topic"))))
          }
        } pipeTo sender()
        
      case CMAddTopicPartitions(topic, brokers, partitions, partitionReplicaList, readVersion) =>
        implicit val ec = longRunningExecutionContext
        val eventualTopicDescription = withKafkaStateActor(KSGetTopicDescription(topic))(identity[Option[TopicDescription]])
        val eventualBrokerList = withKafkaStateActor(KSGetBrokers)(identity[BrokerList])
        eventualTopicDescription.map { topicDescriptionOption =>
          topicDescriptionOption.fold {
            Future.successful(CMCommandResult(Failure(new IllegalArgumentException(s"Topic doesn't exist : $topic"))))
          } { td =>
            eventualBrokerList.flatMap {
              bl => {
                val brokerSet = bl.list.map(_.id).toSet
                withKafkaCommandActor(KCAddTopicPartitions(topic, brokers.filter(brokerSet.apply).toSet, partitions, partitionReplicaList, readVersion))
                {
                  kcResponse: KCCommandResult =>
                    CMCommandResult(kcResponse.result)
                }
              }
            }
          }
        } pipeTo sender()

      case CMAddMultipleTopicsPartitions(topicsAndReplicas, brokers, partitions, readVersions) =>
        implicit val ec = longRunningExecutionContext
        val eventualBrokerList = withKafkaStateActor(KSGetBrokers)(identity[BrokerList])
        val eventualDescriptions = withKafkaStateActor(KSGetTopicDescriptions(topicsAndReplicas.map(x=>x._1).toSet))(identity[TopicDescriptions])
        eventualDescriptions.map { topicDescriptions =>
          val topicsWithoutDescription = topicsAndReplicas.map(x=>x._1).filter{t => !topicDescriptions.descriptions.map(td => td.topic).contains(t) }
          require(topicsWithoutDescription.isEmpty, "Topic(s) don't exist: [%s]".format(topicsWithoutDescription.mkString(", ")))
          eventualBrokerList.flatMap {
            bl => {
              val brokerSet = bl.list.map(_.id).toSet
              val nonExistentBrokers = getNonExistentBrokers(bl, brokers)
              require(nonExistentBrokers.isEmpty, "Nonexistent broker(s) selected: [%s]".format(nonExistentBrokers.mkString(", ")))
              withKafkaCommandActor(KCAddMultipleTopicsPartitions(topicsAndReplicas, brokers.filter(brokerSet.apply), partitions, readVersions))
              {
                kcResponse: KCCommandResult =>
                  CMCommandResult(kcResponse.result)
              }
            }
          }
        } pipeTo sender()

      case CMUpdateTopicConfig(topic, config, readVersion) =>
        implicit val ec = longRunningExecutionContext
        val eventualTopicDescription = withKafkaStateActor(KSGetTopicDescription(topic))(identity[Option[TopicDescription]])
        val eventualBrokerList = withKafkaStateActor(KSGetBrokers)(identity[BrokerList])
        eventualTopicDescription.map { topicDescriptionOption =>
          topicDescriptionOption.fold {
            Future.successful(CMCommandResult(Failure(new IllegalArgumentException(s"Topic doesn't exist : $topic"))))
          } { td =>
            eventualBrokerList.flatMap {
              bl => {
                val brokerSet = bl.list.map(_.id).toSet
                withKafkaCommandActor(KCUpdateTopicConfig(topic, config, readVersion))
                {
                  kcResponse: KCCommandResult =>
                    CMCommandResult(kcResponse.result)
                }
              }
            }
          }
        } pipeTo sender()

      case CMGeneratePartitionAssignments(topics, brokers, replicationFactor) =>
        implicit val ec = longRunningExecutionContext
        val topicCheckFutureBefore = checkTopicsUnderAssignment(topics)

        val generated: Future[IndexedSeq[(String, Map[Int, Seq[Int]])]] = topicCheckFutureBefore.flatMap { _ =>
          val eventualBrokerList = withKafkaStateActor(KSGetBrokers)(identity[BrokerList])
          val eventualDescriptions = withKafkaStateActor(KSGetTopicDescriptions(topics))(identity[TopicDescriptions])
          for {
            bl <- eventualBrokerList
            tds <- eventualDescriptions
            tis = tds.descriptions.map(TopicIdentity.from(bl, _, None, None, clusterContext, None))
          } yield {
            // check if any nonexistent broker got selected for reassignment
            val nonExistentBrokers = getNonExistentBrokers(bl, brokers)
            require(nonExistentBrokers.isEmpty, "Nonexistent broker(s) selected: [%s]".format(nonExistentBrokers.mkString(", ")))
            tis.map(ti => (ti.topic, adminUtils.assignReplicasToBrokers(
              brokers,
              ti.partitions,
              replicationFactor.getOrElse(ti.replicationFactor))))
          }
        }

        val result: Future[IndexedSeq[Try[Unit]]] = for {
          list <- generated
          _ <- checkTopicsUnderAssignment(topics) //check again
        } yield {
          modify {
            list.map { case (topic, assignments: Map[Int, Seq[Int]]) =>
              updateAssignmentInZk(topic, assignments)
            }
          }
        }
        result.map(CMCommandResults.apply) pipeTo sender()

      case CMManualPartitionAssignments(assignments) =>
        implicit val ec = longRunningExecutionContext
        val result = Future {
          modify {
            assignments.map { case (topic, assignment) =>
              updateAssignmentInZk(topic, assignment.toMap)
            }
          }.toIndexedSeq
        }
        result.map(CMCommandResults.apply) pipeTo sender()

      case CMRunPreferredLeaderElection(topics) =>
        implicit val ec = longRunningExecutionContext
        val eventualBrokerList = withKafkaStateActor(KSGetBrokers)(identity[BrokerList])
        val eventualDescriptions = withKafkaStateActor(KSGetTopicDescriptions(topics))(identity[TopicDescriptions])
        val preferredLeaderElections = for {
          bl <- eventualBrokerList
          tds <- eventualDescriptions
          tis = tds.descriptions.map(TopicIdentity.from(bl, _, None, None, clusterContext, None))
          toElect = tis.flatMap(ti => ti.partitionsIdentity.values.filter(!_.isPreferredLeader).map(tpi => new TopicPartition(ti.topic, tpi.partNum))).toSet
        } yield toElect
        preferredLeaderElections.map { toElect =>
          withKafkaCommandActor(KCPreferredReplicaLeaderElection(toElect)) { kcResponse: KCCommandResult =>
            CMCommandResult(kcResponse.result)
          }
        } pipeTo sender()

      case CMSchedulePreferredLeaderElection(schedule) =>
        implicit val ec = longRunningExecutionContext
        writeScheduleLeaderElectionToZk(schedule)

      case CMRunReassignPartition(topics, forceSet) =>
        implicit val ec = longRunningExecutionContext
        val eventualBrokerList = withKafkaStateActor(KSGetBrokers)(identity[BrokerList])
        val eventualDescriptions = withKafkaStateActor(KSGetTopicDescriptions(topics))(identity[TopicDescriptions])
        val topicsAndReassignments = for {
          bl <- eventualBrokerList
          tds <- eventualDescriptions
          tis = tds.descriptions.map(TopicIdentity.from(bl, _, None, None, clusterContext, None))
        } yield {
          val reassignments = tis.map { ti =>
            val topicZkPath = zkPathFrom(baseTopicsZkPath, ti.topic)
            Try {
              val assignments = getGeneratedPartitionAssignments(ti.topic)
              val nonExistentBrokers = getNonExistentBrokers(bl, assignments)
              require(nonExistentBrokers.isEmpty, "The assignments contain nonexistent broker(s): [%s]".format(nonExistentBrokers.mkString(", ")))
              for {
              newTi <- TopicIdentity.reassignReplicas(ti, assignments)
              } yield newTi
            }.flatten
          }
          (tis, reassignments)
        }
        topicsAndReassignments.map { case (topicIdentities, reassignments) =>
          val topicsMap = topicIdentities.map(ti => (ti.topic, ti)).toMap
          val reassignmentsMap = reassignments.filter(_.isSuccess).map(_.toOption).flatten.map(ti => (ti.topic, ti)).toMap
          val failures: IndexedSeq[Try[Unit]] = reassignments.filter(_.isFailure).map(_.flatMap(ti => Try[Unit]((): Unit)))
          withKafkaCommandActor(KCReassignPartition(topicsMap, reassignmentsMap, forceSet)) { kcResponse: KCCommandResult =>
            CMCommandResults(failures ++ IndexedSeq(kcResponse.result))
          }
        } pipeTo sender()

      case CMDeleteLogkafka(logkafka_id, log_path) =>
        implicit val ec = longRunningExecutionContext
        val eventualLogkafkaConfig = withLogkafkaStateActor(LKSGetLogkafkaConfig(logkafka_id))(identity[Option[LogkafkaConfig]])
        eventualLogkafkaConfig.map { logkafkaConfigOption =>
          logkafkaConfigOption.fold {
            Future.successful(CMCommandResult(Failure(new IllegalArgumentException(s"LogkafkaId doesn't exists : $logkafka_id"))))
          } { td =>
            withLogkafkaCommandActor(LKCDeleteLogkafka(logkafka_id, log_path, logkafkaConfigOption)) {
              lkcResponse: LKCCommandResult =>
                CMCommandResult(lkcResponse.result)
            }
          }
        } pipeTo sender()

      case CMCreateLogkafka(logkafka_id, log_path, config) =>
        implicit val ec = longRunningExecutionContext
        val eventualLogkafkaConfig = withLogkafkaStateActor(LKSGetLogkafkaConfig(logkafka_id))(identity[Option[LogkafkaConfig]])
        eventualLogkafkaConfig.map { logkafkaConfigOption =>
            withLogkafkaCommandActor(LKCCreateLogkafka(logkafka_id, log_path, config, logkafkaConfigOption)) {
              lkcResponse: LKCCommandResult =>
                CMCommandResult(lkcResponse.result)
            }
        } pipeTo sender()

      case CMUpdateLogkafkaConfig(logkafka_id, log_path, config, checkConfig) =>
        implicit val ec = longRunningExecutionContext
        val eventualLogkafkaConfig = withLogkafkaStateActor(LKSGetLogkafkaConfig(logkafka_id))(identity[Option[LogkafkaConfig]])
        eventualLogkafkaConfig.map { logkafkaConfigOption =>
            withLogkafkaCommandActor(LKCUpdateLogkafkaConfig(logkafka_id, log_path, config, logkafkaConfigOption, checkConfig)) {
              lkcResponse: LKCCommandResult =>
                CMCommandResult(lkcResponse.result)
            }
        } pipeTo sender()

      case any: Any => log.warning("cma : processCommandRequest : Received unknown message: {}", any)
    }
  }

  private[this]  def withKafkaStateActor[Input,Output,FOutput]
  (msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output], ec: ExecutionContext) : Future[FOutput] = {
    context.actorSelection(kafkaStateActor).ask(msg).mapTo[Output].map(fn)
  }

  private[this]  def withLogkafkaStateActor[Input,Output,FOutput]
  (msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output], ec: ExecutionContext) : Future[FOutput] = {
    context.actorSelection(logkafkaStateActor.get).ask(msg).mapTo[Output].map(fn)
  }

  private[this] def withBrokerViewCacheActor[Input,Output,FOutput]
  (msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output], ec: ExecutionContext) : Future[FOutput] = {
    context.actorSelection(brokerViewCacheActor).ask(msg).mapTo[Output].map(fn)
  }

  private[this] def withLogkafkaViewCacheActor[Input,Output,FOutput]
  (msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output], ec: ExecutionContext) : Future[FOutput] = {
    context.actorSelection(logkafkaViewCacheActor.get).ask(msg).mapTo[Output].map(fn)
  }

  private[this] def withKafkaCommandActor[Input,Output,FOutput]
  (msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output], ec: ExecutionContext) : Future[FOutput] = {
    context.actorSelection(kafkaCommandActor).ask(msg).mapTo[Output].map(fn)
  }

  private[this] def withLogkafkaCommandActor[Input,Output,FOutput]
  (msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output], ec: ExecutionContext) : Future[FOutput] = {
    context.actorSelection(logkafkaCommandActor.get).ask(msg).mapTo[Output].map(fn)
  }

  private[this] def modify[T](fn: => T): T = {
    try {
      if(mutex.acquire(cmConfig.mutexTimeoutMillis,TimeUnit.MILLISECONDS)) {
        fn
      } else {
        throw new RuntimeException("Failed to acquire mutex for cluster manager command")
      }
    } finally {
      if(mutex.isAcquiredInThisProcess) {
        mutex.release()
      }
    }
  }

  private[this] def getNonExistentBrokers(availableBrokers: BrokerList, selectedBrokers: Set[Int]): Set[Int] = {
    val availableBrokerIds: Set[Int] = availableBrokers.list.map(_.id).toSet
    selectedBrokers filter { b: Int => !availableBrokerIds.contains(b) }
  }

  private[this] def getNonExistentBrokers(availableBrokers: BrokerList, assignments: Map[Int, Seq[Int]]): Set[Int] = {
    val brokersAssigned = assignments.flatMap({ case  (pt, bl) => bl }).toSet
    getNonExistentBrokers(availableBrokers, brokersAssigned)
  }

  private[this] def getTopicsUnderReassignment(reassignPartitions: Option[ReassignPartitions], topicsToBeReassigned: Set[String]): Set[String] = {
    val topicsUnderReassignment = reassignPartitions.map { asgn =>
      asgn.endTime.map(_ => Set[String]()).getOrElse{
        asgn.partitionsToBeReassigned.map { case (t,s) => t.topic}.toSet
      }
    }.getOrElse(Set[String]())
    topicsToBeReassigned.intersect(topicsUnderReassignment)
  }
  
  private[this] def checkTopicsUnderAssignment(topicsToBeReassigned: Set[String])(implicit ec: ExecutionContext) : Future[Unit] = {
    val eventualReassignPartitions = withKafkaStateActor(KSGetReassignPartition)(identity[Option[ReassignPartitions]])
    for {
      rp <- eventualReassignPartitions
    } yield {
      // check if any topic undergoing reassignment got selected for reassignment
      val topicsUndergoingReassignment = getTopicsUnderReassignment(rp, topicsToBeReassigned)
      require(topicsUndergoingReassignment.isEmpty, "Topic(s) already undergoing reassignment(s): [%s]"
        .format(topicsUndergoingReassignment.mkString(", ")))
    }
  }
}
