/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package scheduler.kafka.manager

import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import akka.actor.{ActorPath, Props}
import akka.pattern._
import akka.util.Timeout
import kafka.manager._
import kafka.manager.utils.AdminUtils
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.zookeeper.CreateMode

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Try

import kafka.manager.ActorModel._

case class SchedulerManagerActorConfig(pinnedDispatcherName: String,
                                baseZkPath : String,
                                curatorConfig: CuratorConfig,
                                schedulerConfig: SchedulerConfig,
                                updatePeriod: FiniteDuration,
                                threadPoolSize: Int = 2,
                                maxQueueSize: Int = 100,
                                // enlarging from 2s to 10s because 2s for Scheduler REST API is not enough
                                askTimeoutMillis: Long = 10000,
                                mutexTimeoutMillis: Int = 4000)

class SchedulerManagerActor(smConfig: SchedulerManagerActorConfig)
  extends BaseQueryCommandActor with CuratorAwareActor with BaseZkPath {

  //this is from base zk path trait
  override def baseZkPath : String = smConfig.baseZkPath

  //this is for curator aware actor
  override def curatorConfig: CuratorConfig = smConfig.curatorConfig

  val longRunningExecutor = new ThreadPoolExecutor(
    smConfig.threadPoolSize, smConfig.threadPoolSize,0L,TimeUnit.MILLISECONDS,new LinkedBlockingQueue[Runnable](smConfig.maxQueueSize))
  val longRunningExecutionContext = ExecutionContext.fromExecutor(longRunningExecutor)

  protected[this] val sharedClusterCurator : CuratorFramework = getCurator(smConfig.schedulerConfig.curatorConfig)
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

  private[this] val adminUtils = new AdminUtils(smConfig.schedulerConfig.version)

  private[this] val kssProps = Props(classOf[KafkaSchedulerStateActor],sharedClusterCurator, adminUtils.isDeleteSupported, smConfig.schedulerConfig)
  private[this] val kafkaSchedulerStateActor : ActorPath = context.actorOf(kssProps.withDispatcher(smConfig.pinnedDispatcherName),"kafka-scheduler-state").path

  private[this] val bvConfig = SchedulerBrokerViewCacheActorConfig(
    kafkaSchedulerStateActor,
    smConfig.schedulerConfig,
    LongRunningPoolConfig(Runtime.getRuntime.availableProcessors(), 1000),
    smConfig.updatePeriod)
  private[this] val bvcProps = Props(classOf[SchedulerBrokerViewCacheActor],bvConfig)
  private[this] val brokerViewCacheActor : ActorPath = context.actorOf(bvcProps,"scheduler-broker-view").path

  private[this] val kscProps = {
    val kscaConfig = KafkaSchedulerCommandActorConfig(
      smConfig.schedulerConfig,
      sharedClusterCurator,
      LongRunningPoolConfig(smConfig.threadPoolSize, smConfig.maxQueueSize),
      smConfig.askTimeoutMillis,
      smConfig.schedulerConfig.version)
    Props(classOf[KafkaSchedulerCommandActor],kscaConfig)
  }
  private[this] val kafkaSchedulerCommandActor : ActorPath = context.actorOf(kscProps,"kafka-scheduler-command").path

  private[this] implicit val timeout: Timeout = FiniteDuration(smConfig.askTimeoutMillis,MILLISECONDS)

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

    log.info("Shutting down scheduler manager topics path cache...")
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
        context.actorSelection(kafkaSchedulerStateActor).forward(ksRequest)

      case bvRequest: BVRequest =>
        context.actorSelection(brokerViewCacheActor).forward(bvRequest)

      case SMGetView =>
        implicit val ec = context.dispatcher
        val eventualBrokerList = withKafkaSchedulerStateActor(SchedulerKSGetBrokers)(identity[SchedulerBrokerList])
        val eventualTopicList = withKafkaSchedulerStateActor(KSGetTopics)(identity[TopicList])
        val result = for {
          bl <- eventualBrokerList
          tl <- eventualTopicList
        } yield SMView(tl.list.size, bl.list.size, smConfig.schedulerConfig)
        result pipeTo sender

      case any: Any => log.warning("sma : processQueryResponse : Received unknown message: {}", any)
    }
  }

  override def processCommandRequest(request: CommandRequest): Unit = {
    request match {
      case SMAddBroker(id, cpus, mem, heap, port, bindAddress, constraints, options, log4jOptions, jvmOptions, stickinessPeriod, failover) =>
        implicit val ec = longRunningExecutionContext

        withKafkaCommandActor(KSCAddBroker(id, cpus, mem, heap, port, bindAddress, constraints, options, log4jOptions, jvmOptions, stickinessPeriod, failover)){
          kcResponse: KCCommandResult =>
            Future.successful(SMCommandResult(kcResponse.result))
        } pipeTo sender()

      case SMUpdateBroker(id, cpus, mem, heap, port, bindAddress, constraints, options, log4jOptions, jvmOptions, stickinessPeriod, failover) =>
        implicit val ec = longRunningExecutionContext

        withKafkaCommandActor(KSCUpdateBroker(id, cpus, mem, heap, port, bindAddress, constraints, options, log4jOptions, jvmOptions, stickinessPeriod, failover)){
          kcResponse: KCCommandResult =>
            Future.successful(SMCommandResult(kcResponse.result))
        } pipeTo sender()

      case SMStartBroker(id) =>
        implicit val ec = longRunningExecutionContext

        withKafkaCommandActor(KSCStartBroker(id)){
          kcResponse: KCCommandResult =>
            Future.successful(SMCommandResult(kcResponse.result))
        } pipeTo sender()

      case SMStopBroker(id) =>
        implicit val ec = longRunningExecutionContext

        withKafkaCommandActor(KSCStopBroker(id)){
          kcResponse: KCCommandResult =>
            Future.successful(SMCommandResult(kcResponse.result))
        } pipeTo sender()

      case SMRemoveBroker(id) =>
        implicit val ec = longRunningExecutionContext

        withKafkaCommandActor(KSCRemoveBroker(id)){
          kcResponse: KCCommandResult =>
            Future.successful(SMCommandResult(kcResponse.result))
        } pipeTo sender()

      case SMRebalanceTopics(ids, topics) =>
        implicit val ec = longRunningExecutionContext

        withKafkaCommandActor(KSCRebalanceTopics(ids, topics)){
          kcResponse: KCCommandResult =>
            Future.successful(SMCommandResult(kcResponse.result))
        } pipeTo sender()

      case any: Any => log.warning("cma : processCommandRequest : Received unknown message: {}", any)
    }
  }

  private[this]  def withKafkaSchedulerStateActor[Input,Output,FOutput]
  (msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output], ec: ExecutionContext) : Future[FOutput] = {
    context.actorSelection(kafkaSchedulerStateActor).ask(msg).mapTo[Output].map(fn)
  }

  private[this] def withKafkaCommandActor[Input,Output,FOutput]
  (msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output], ec: ExecutionContext) : Future[FOutput] = {
    context.actorSelection(kafkaSchedulerCommandActor).ask(msg).mapTo[Output].map(fn)
  }

  private[this] def modify[T](fn: => T): T = {
    try {
      mutex.acquire(smConfig.mutexTimeoutMillis,TimeUnit.MILLISECONDS)
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
}
