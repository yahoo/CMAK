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
import kafka.manager.utils.{AdminUtils, TopicAndPartition}

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
  extends CuratorAwareActor(cmConfig.curatorConfig) with BaseZkPath {

  //this is from base zk path trait
  override val baseZkPath : String = cmConfig.baseZkPath

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

  private[this] val ksProps = Props(classOf[KafkaStateActor],sharedClusterCurator)
  private[this] val kafkaStateActor : ActorPath = context.actorOf(ksProps.withDispatcher(cmConfig.pinnedDispatcherName),"kafka-state").path

  private[this] val bvcProps = Props(classOf[BrokerViewCacheActor],kafkaStateActor,cmConfig.updatePeriod)
  private[this] val brokerViewCacheActor : ActorPath = context.actorOf(bvcProps,"broker-view").path

  private[this] val kcProps = {
    val kcaConfig = KafkaCommandActorConfig(sharedClusterCurator,cmConfig.threadPoolSize,cmConfig.maxQueueSize,cmConfig.askTimeoutMillis)
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
      case any: Any => log.warning("Received unknown message: {}", any)
    }
  }

  override def processActorRequest(request: ActorRequest): Unit = {
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
        } yield CMView(tl.list.size, bl.list.size,cmConfig.clusterConfig)
        result pipeTo sender

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

      case CMGeneratePartitionAssignments(topics) =>
        implicit val ec = longRunningExecutionContext
        val eventualBrokerList = withKafkaStateActor(KSGetBrokers)(identity[BrokerList])
        val eventualDescriptions = withKafkaStateActor(KSGetTopicDescriptions(topics))(identity[TopicDescriptions])
        val generated: Future[IndexedSeq[(String, Map[Int, Seq[Int]])]] = for {
          bl <- eventualBrokerList
          tds <- eventualDescriptions
          tis = tds.descriptions.map(TopicIdentity.from(bl, _))
        } yield {
          tis.map(ti => (ti.topic, AdminUtils.assignReplicasToBrokers(
            bl.list.map(_.id.toInt),
            ti.partitions,
            ti.replicationFactor)))
        }

        val result = generated.map { list =>
          modify {
            list.map { case (topic, assignments: Map[Int, Seq[Int]]) =>
              Try {
                val topicZkPath = zkPathFrom(baseTopicsZkPath, topic)
                val data = serializeAssignments(assignments)
                Option(clusterManagerTopicsPathCache.getCurrentData(topicZkPath)).fold[Unit] {
                  log.info(s"Creating and saving generated data $topicZkPath")
                  curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(topicZkPath, data)
                } { _ =>
                  log.info(s"Updating generated data $topicZkPath")
                  curator.setData().forPath(topicZkPath, data)
                }
              }
            }
          }
        }
        result.map(CMCommandResults.apply) pipeTo sender()

      case CMRunPreferredLeaderElection(topics) =>
        implicit val ec = longRunningExecutionContext
        val eventualBrokerList = withKafkaStateActor(KSGetBrokers)(identity[BrokerList])
        val eventualDescriptions = withKafkaStateActor(KSGetTopicDescriptions(topics))(identity[TopicDescriptions])
        val preferredLeaderElections = for {
          bl <- eventualBrokerList
          tds <- eventualDescriptions
          tis = tds.descriptions.map(TopicIdentity.from(bl, _))
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
          tis = tds.descriptions.map(TopicIdentity.from(bl, _))
        } yield {
          val reassignments = tis.map { ti =>
            val topicZkPath = zkPathFrom(baseTopicsZkPath, ti.topic)
            Try {
              Option(clusterManagerTopicsPathCache.getCurrentData(topicZkPath)).fold {
                throw new IllegalArgumentException(s"No generated assignment found for topic ${ti.topic}")
              } { childData =>
                val assignments = deserializeAssignments(childData.getData)
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

      case any: Any => log.warning("Received unknown message: {}", any)
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
}
