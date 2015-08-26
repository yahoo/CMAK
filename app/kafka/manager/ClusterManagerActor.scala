/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import java.nio.charset.StandardCharsets
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit, ThreadPoolExecutor}

import akka.pattern._
import akka.actor.{ActorPath, Props}
import akka.util.Timeout
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.zookeeper.CreateMode
import kafka.common.TopicAndPartition
import kafka.manager.utils.AdminUtils

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.concurrent.duration._
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

import ActorModel._
import ClusterManagerActor._

case class ClusterManagerActorConfig(pinnedDispatcherName: String,
                                baseZkPath : String,
                                curatorConfig: CuratorConfig,
                                clusterConfig: ClusterConfig,
                                updatePeriod: FiniteDuration,
                                threadPoolSize: Int = 2,
                                maxQueueSize: Int = 100,
                                askTimeoutMillis: Long = 2000,
                                mutexTimeoutMillis: Int = 4000)

class ClusterManagerActor(cmConfig: ClusterManagerActorConfig)
  extends BaseQueryCommandActor with CuratorAwareActor with BaseZkPath {

  //this is from base zk path trait
  override def baseZkPath : String = cmConfig.baseZkPath

  //this is for curator aware actor
  override def curatorConfig: CuratorConfig = cmConfig.curatorConfig

  val longRunningExecutor = new ThreadPoolExecutor(
    cmConfig.threadPoolSize, cmConfig.threadPoolSize,0L,TimeUnit.MILLISECONDS,new LinkedBlockingQueue[Runnable](cmConfig.maxQueueSize))
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

  private[this] val ksConfig = KafkaStateActorConfig(
    sharedClusterCurator,
    adminUtils.isDeleteSupported,
    cmConfig.clusterConfig,
    LongRunningPoolConfig(Runtime.getRuntime.availableProcessors(), 1000))
  private[this] val ksProps = Props(classOf[KafkaStateActor],ksConfig)
  private[this] val kafkaStateActor : ActorPath = context.actorOf(ksProps.withDispatcher(cmConfig.pinnedDispatcherName),"kafka-state").path

  private[this] val bvConfig = BrokerViewCacheActorConfig(
    kafkaStateActor, 
    cmConfig.clusterConfig, 
    LongRunningPoolConfig(Runtime.getRuntime.availableProcessors(), 1000),
    cmConfig.updatePeriod)
  private[this] val bvcProps = Props(classOf[BrokerViewCacheActor],bvConfig)
  private[this] val brokerViewCacheActor : ActorPath = context.actorOf(bvcProps,"broker-view").path

  private[this] val kcProps = {
    val kcaConfig = KafkaCommandActorConfig(
      sharedClusterCurator,
      LongRunningPoolConfig(cmConfig.threadPoolSize, cmConfig.maxQueueSize),
      cmConfig.askTimeoutMillis,
      cmConfig.clusterConfig.version)
    Props(classOf[KafkaCommandActor],kcaConfig)
  }
  private[this] val kafkaCommandActor : ActorPath = context.actorOf(kcProps,"kafka-command").path

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

      case bvRequest: BVRequest =>
        context.actorSelection(brokerViewCacheActor).forward(bvRequest)

      case CMGetView =>
        implicit val ec = context.dispatcher
        val eventualBrokerList = withKafkaStateActor(KSGetBrokers)(identity[BrokerList])
        val eventualTopicList = withKafkaStateActor(KSGetTopics)(identity[TopicList])
        val result = for {
          bl <- eventualBrokerList
          tl <- eventualTopicList
        } yield CMView(tl.list.size, bl.list.size, cmConfig.clusterConfig)
        result pipeTo sender

      case CMGetTopicIdentity(topic) =>
        implicit val ec = context.dispatcher
        val eventualBrokerList = withKafkaStateActor(KSGetBrokers)(identity[BrokerList])
        val eventualTopicMetrics : Future[Option[BrokerMetrics]] = {
          if(cmConfig.clusterConfig.jmxEnabled) {
            withBrokerViewCacheActor(BVGetTopicMetrics(topic))(identity[Option[BrokerMetrics]])
          } else {
            Future.successful(None)
          }
        }
        val eventualTopicDescription = withKafkaStateActor(KSGetTopicDescription(topic))(identity[Option[TopicDescription]])
        val result: Future[Option[CMTopicIdentity]] = for {
          bl <- eventualBrokerList
          tm <- eventualTopicMetrics
          tdO <- eventualTopicDescription
        } yield tdO.map( td => CMTopicIdentity(Try(TopicIdentity.from(bl,td,tm,cmConfig.clusterConfig))))
        result pipeTo sender

      case CMGetConsumerIdentity(consumer) =>
        implicit val ec = context.dispatcher
        val eventualConsumerDescription = withKafkaStateActor(KSGetConsumerDescription(consumer))(identity[Option[ConsumerDescription]])
        val result: Future[Option[CMConsumerIdentity]] = for {
          cdO <- eventualConsumerDescription
          ciO = cdO.map( cd => CMConsumerIdentity(Try(ConsumerIdentity.from(cd,cmConfig.clusterConfig))))
        } yield ciO
        result pipeTo sender

      case CMGetConsumedTopicState(consumer, topic) =>
        implicit val ec = context.dispatcher
        val eventualConsumedTopicDescription = withKafkaStateActor(
          KSGetConsumedTopicDescription(consumer,topic)
        )(identity[ConsumedTopicDescription])
        val result: Future[CMConsumedTopic] = eventualConsumedTopicDescription.map{
          ctd: ConsumedTopicDescription =>  CMConsumedTopic(Try(ConsumedTopicState.from(ctd)))
        }
        result pipeTo sender

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
              bl => withKafkaCommandActor(KCCreateTopic(topic, bl.list.map(_.id.toInt), partitions, replication, config)) {
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
                withKafkaCommandActor(KCAddTopicPartitions(topic, brokers.filter(brokerSet.apply), partitions, partitionReplicaList, readVersion))
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

      case CMGeneratePartitionAssignments(topics, brokers) =>
        implicit val ec = longRunningExecutionContext
        val eventualBrokerList = withKafkaStateActor(KSGetBrokers)(identity[BrokerList])
        val eventualDescriptions = withKafkaStateActor(KSGetTopicDescriptions(topics))(identity[TopicDescriptions])
        val eventualReassignPartitions = withKafkaStateActor(KSGetReassignPartition)(identity[Option[ReassignPartitions]])
        val generated: Future[IndexedSeq[(String, Map[Int, Seq[Int]])]] = for {
          bl <- eventualBrokerList
          tds <- eventualDescriptions
          rp <- eventualReassignPartitions
          tis = tds.descriptions.map(TopicIdentity.from(bl, _, None,cmConfig.clusterConfig))
        } yield {
          bl.list.map(_.id.toInt)
          // check if any topic undergoing reassignment got selected for reassignment
          val topicsUndergoingReassignment = getTopicsUnderReassignment(rp, topics)
          require(topicsUndergoingReassignment.isEmpty, "Topic(s) already undergoing reassignment(s): [%s]".format(topicsUndergoingReassignment.mkString(", ")))
          // check if any nonexistent broker got selected for reassignment
          val nonExistentBrokers = getNonExistentBrokers(bl, brokers)
          require(nonExistentBrokers.isEmpty, "Nonexistent broker(s) selected: [%s]".format(nonExistentBrokers.mkString(", ")))
          tis.map(ti => (ti.topic, adminUtils.assignReplicasToBrokers(
            brokers,
            ti.partitions,
            ti.replicationFactor)))
        }

        val result: Future[IndexedSeq[Try[Unit]]] = generated.map { list =>
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
          } toIndexedSeq
        }
        result.map(CMCommandResults.apply) pipeTo sender()

      case CMRunPreferredLeaderElection(topics) =>
        implicit val ec = longRunningExecutionContext
        val eventualBrokerList = withKafkaStateActor(KSGetBrokers)(identity[BrokerList])
        val eventualDescriptions = withKafkaStateActor(KSGetTopicDescriptions(topics))(identity[TopicDescriptions])
        val preferredLeaderElections = for {
          bl <- eventualBrokerList
          tds <- eventualDescriptions
          tis = tds.descriptions.map(TopicIdentity.from(bl, _, None, cmConfig.clusterConfig))
          toElect = tis.map(ti => ti.partitionsIdentity.values.filter(!_.isPreferredLeader).map(tpi => TopicAndPartition(ti.topic, tpi.partNum))).flatten.toSet
        } yield toElect
        preferredLeaderElections.map { toElect =>
          withKafkaCommandActor(KCPreferredReplicaLeaderElection(toElect)) { kcResponse: KCCommandResult =>
            CMCommandResult(kcResponse.result)
          }
        } pipeTo sender()

      case CMRunReassignPartition(topics) =>
        implicit val ec = longRunningExecutionContext
        val eventualBrokerList = withKafkaStateActor(KSGetBrokers)(identity[BrokerList])
        val eventualDescriptions = withKafkaStateActor(KSGetTopicDescriptions(topics))(identity[TopicDescriptions])
        val topicsAndReassignments = for {
          bl <- eventualBrokerList
          tds <- eventualDescriptions
          tis = tds.descriptions.map(TopicIdentity.from(bl, _, None, cmConfig.clusterConfig))
        } yield {
          val reassignments = tis.map { ti =>
            val topicZkPath = zkPathFrom(baseTopicsZkPath, ti.topic)
            Try {
              Option(clusterManagerTopicsPathCache.getCurrentData(topicZkPath)).fold {
                throw new IllegalArgumentException(s"No generated assignment found for topic ${ti.topic}")
              } { childData =>
                val assignments = deserializeAssignments(childData.getData)
                // check if any nonexistent broker got selected for reassignment
                val nonExistentBrokers = getNonExistentBrokers(bl, assignments)
                require(nonExistentBrokers.isEmpty, "The assignments contain nonexistent broker(s): [%s]".format(nonExistentBrokers.mkString(", ")))
                TopicIdentity.reassignReplicas(ti, assignments)
              }
            }.flatten
          }
          (tis, reassignments)
        }
        topicsAndReassignments.map { case (topicIdentities, reassignments) =>
          val topicsMap = topicIdentities.map(ti => (ti.topic, ti)).toMap
          val reassignmentsMap = reassignments.filter(_.isSuccess).map(_.toOption).flatten.map(ti => (ti.topic, ti)).toMap
          val failures: IndexedSeq[Try[Unit]] = reassignments.filter(_.isFailure).map(_.flatMap(ti => Try[Unit]((): Unit)))
          withKafkaCommandActor(KCReassignPartition(topicsMap, reassignmentsMap)) { kcResponse: KCCommandResult =>
            CMCommandResults(failures ++ IndexedSeq(kcResponse.result))
          }
        } pipeTo sender()

      case any: Any => log.warning("cma : processCommandRequest : Received unknown message: {}", any)
    }
  }

  private[this]  def withKafkaStateActor[Input,Output,FOutput]
  (msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output], ec: ExecutionContext) : Future[FOutput] = {
    context.actorSelection(kafkaStateActor).ask(msg).mapTo[Output].map(fn)
  }

  private[this] def withBrokerViewCacheActor[Input,Output,FOutput]
  (msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output], ec: ExecutionContext) : Future[FOutput] = {
    context.actorSelection(brokerViewCacheActor).ask(msg).mapTo[Output].map(fn)
  }

  private[this] def withKafkaCommandActor[Input,Output,FOutput]
  (msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output], ec: ExecutionContext) : Future[FOutput] = {
    context.actorSelection(kafkaCommandActor).ask(msg).mapTo[Output].map(fn)
  }

  private[this] def modify[T](fn: => T): T = {
    try {
      mutex.acquire(cmConfig.mutexTimeoutMillis,TimeUnit.MILLISECONDS)
      fn
    } finally {
      if(mutex.isAcquiredInThisProcess) {
        mutex.release()
      }
    }
  }

  def getNonExistentBrokers(availableBrokers: BrokerList, selectedBrokers: Seq[Int]): Seq[Int] = {
    val availableBrokerIds: Set[Int] = availableBrokers.list.map(_.id.toInt).toSet
    selectedBrokers filter { b: Int => !availableBrokerIds.contains(b) }
  }

  def getNonExistentBrokers(availableBrokers: BrokerList, assignments: Map[Int, Seq[Int]]): Seq[Int] = {
    val brokersAssigned = assignments.flatMap({ case  (pt, bl) => bl }).toSet.toSeq
    getNonExistentBrokers(availableBrokers, brokersAssigned)
  }

  def getTopicsUnderReassignment(reassignPartitions: Option[ReassignPartitions], topicsToBeReassigned: Set[String]): Set[String] = {
    val topicsUnderReassignment = reassignPartitions.map { asgn =>
      asgn.endTime.map(_ => Set[String]()).getOrElse{
        asgn.partitionsToBeReassigned.map { case (t,s) => t.topic}.toSet
      }
    }.getOrElse(Set[String]())
    topicsToBeReassigned.intersect(topicsUnderReassignment)
  }
}
