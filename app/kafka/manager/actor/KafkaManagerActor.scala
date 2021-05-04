/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.actor

import akka.actor.{ActorPath, Props}
import akka.pattern._
import kafka.manager.actor.cluster.{ClusterManagerActor, ClusterManagerActorConfig}
import kafka.manager.base.{BaseQueryCommandActor, BaseZkPath, CuratorAwareActor}
import kafka.manager.model.ActorModel.CMShutdown
import kafka.manager.model.{ClusterConfig, ClusterTuning, CuratorConfig}
import kafka.manager.utils.ZkUtils
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.zookeeper.CreateMode

import java.util.Properties
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * @author hiral
 */

object KafkaManagerActor {
  val ZkRoot : String = "/kafka-manager"

  def getClusterPath(config: ClusterConfig) : String = s"$ZkRoot/${config.name}"

}

import kafka.manager.model.ActorModel._

import scala.collection.JavaConverters._
import scala.concurrent.duration._

case class KafkaManagerActorConfig(curatorConfig: CuratorConfig
                                   , baseZkPath : String = KafkaManagerActor.ZkRoot
                                   , pinnedDispatcherName : String = "pinned-dispatcher"
                                   , startDelayMillis: Long = 1000
                                   , threadPoolSize: Int = 2
                                   , mutexTimeoutMillis: Int = 4000
                                   , maxQueueSize: Int = 100
                                   , kafkaManagerUpdatePeriod: FiniteDuration = 10 seconds
                                   , deleteClusterUpdatePeriod: FiniteDuration = 10 seconds
                                   , deletionBatchSize : Int = 2
                                   , clusterActorsAskTimeoutMillis: Int = 2000
                                   , simpleConsumerSocketTimeoutMillis : Int = 10000
                                   , defaultTuning: ClusterTuning
                                   , consumerProperties: Option[Properties]
                                  )
class KafkaManagerActor(kafkaManagerConfig: KafkaManagerActorConfig)
  extends BaseQueryCommandActor with CuratorAwareActor with BaseZkPath {

  //this is for baze zk path trait
  override def baseZkPath : String = kafkaManagerConfig.baseZkPath

  //this is for curator aware actor
  override def curatorConfig: CuratorConfig = kafkaManagerConfig.curatorConfig
  
  private[this] val baseClusterZkPath = zkPath("clusters")
  private[this] val configsZkPath = zkPath("configs")
  private[this] val deleteClustersZkPath = zkPath("deleteClusters")

  log.info(s"zk=${kafkaManagerConfig.curatorConfig.zkConnect}")
  log.info(s"baseZkPath=$baseZkPath")

  //create kafka manager base path
  Try(curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(baseZkPath))
  require(curator.checkExists().forPath(baseZkPath) != null,s"Kafka manager base path not found : $baseZkPath")

  //create kafka manager base clusters path
  Try(curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(baseClusterZkPath))
  require(curator.checkExists().forPath(baseClusterZkPath) != null,s"Kafka manager base clusters path not found : $baseClusterZkPath")

  //create kafka manager delete clusters path
  Try(curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(deleteClustersZkPath))
  require(curator.checkExists().forPath(deleteClustersZkPath) != null,s"Kafka manager delete clusters path not found : $deleteClustersZkPath")

  //create kafka manager configs path
  Try(curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(configsZkPath))
  require(curator.checkExists().forPath(configsZkPath) != null,s"Kafka manager configs path not found : $configsZkPath")

  private[this] val longRunningExecutor = new ThreadPoolExecutor(
    kafkaManagerConfig.threadPoolSize,
    kafkaManagerConfig.threadPoolSize,
    0L,
    TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable](kafkaManagerConfig.maxQueueSize))

  private[this] val longRunningExecutionContext = ExecutionContext.fromExecutor(longRunningExecutor)

  private[this] val kafkaManagerPathCache = new PathChildrenCache(curator,configsZkPath,true)

  private[this] val mutex = new InterProcessSemaphoreMutex(curator, zkPath("mutex"))

  private[this] val dcProps = {
    val dcConfig = DeleteClusterActorConfig(
      curator,
      deleteClustersZkPath,
      baseClusterZkPath,
      configsZkPath,
      kafkaManagerConfig.deleteClusterUpdatePeriod,
      kafkaManagerConfig.deletionBatchSize)
    Props(classOf[DeleteClusterActor],dcConfig)
  }

  private[this] val deleteClustersActor: ActorPath = context.actorOf(dcProps.withDispatcher(kafkaManagerConfig.pinnedDispatcherName),"delete-cluster").path

  private[this] val deleteClustersPathCache = new PathChildrenCache(curator,deleteClustersZkPath,true)

  private[this] val pathCacheListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      log.debug(s"Got event : ${event.getType} path=${Option(event.getData).map(_.getPath)}")
      event.getType match {
        case PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED =>
          self ! KMUpdateState
          self ! KMPruneClusters
        case PathChildrenCacheEvent.Type.CHILD_ADDED |
             PathChildrenCacheEvent.Type.CHILD_UPDATED =>
          self ! KMUpdateState
        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
          self ! KMPruneClusters
        case _ => //don't care
      }
    }
  }

  private[this] var lastUpdateMillis: Long = 0L

  private[this] var clusterManagerMap : Map[String,ActorPath] = Map.empty
  private[this] var clusterConfigMap : Map[String,ClusterConfig] = Map.empty
  private[this] var pendingClusterConfigMap : Map[String,ClusterConfig] = Map.empty

  private[this] def modify(fn: => Any) : Unit = {
    if(longRunningExecutor.getQueue.remainingCapacity() == 0) {
      Future.successful(KMCommandResult(Try(throw new UnsupportedOperationException("Long running executor blocking queue is full!"))))
    } else {
      implicit val ec = longRunningExecutionContext
      Future {
        try {
          log.debug(s"Acquiring kafka manager mutex...")
          if(mutex.acquire(kafkaManagerConfig.mutexTimeoutMillis,TimeUnit.MILLISECONDS)) {
            KMCommandResult(Try {
              fn
            })
          } else {
            throw new RuntimeException("Failed to acquire lock for kafka manager command")
          }
        } finally {
          if(mutex.isAcquiredInThisProcess) {
            log.debug(s"Releasing kafka manger mutex...")
            mutex.release()
          }
        }
      } pipeTo sender()
    }
  }

  @scala.throws[Exception](classOf[Exception])
  override def preStart() = {
    super.preStart()

    import scala.concurrent.duration._
    log.info("Started actor %s".format(self.path))

    log.info("Starting delete clusters path cache...")
    deleteClustersPathCache.start(StartMode.BUILD_INITIAL_CACHE)

    log.info("Starting kafka manager path cache...")
    kafkaManagerPathCache.start(StartMode.BUILD_INITIAL_CACHE)

    log.info("Adding kafka manager path cache listener...")
    kafkaManagerPathCache.getListenable.addListener(pathCacheListener)

    implicit val ec = longRunningExecutionContext
    //schedule periodic forced update
    context.system.scheduler.scheduleAtFixedRate(
      Duration(kafkaManagerConfig.startDelayMillis,TimeUnit.MILLISECONDS),kafkaManagerConfig.kafkaManagerUpdatePeriod) {
      () => {
        self ! KMUpdateState
      }
    }
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

    log.info("Removing kafka manager path cache listener...")
    Try(kafkaManagerPathCache.getListenable.removeListener(pathCacheListener))

    log.info("Shutting down long running executor...")
    Try(longRunningExecutor.shutdown())

    log.info("Shutting down kafka manager path cache...")
    Try(kafkaManagerPathCache.close())

    log.info("Shutting down delete clusters path cache...")
    Try(deleteClustersPathCache.close())

    super.postStop()
  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case any: Any => log.warning("kma : processActorResponse : Received unknown message: {}", any)
    }
  }

  
  override def processQueryRequest(request: QueryRequest): Unit = {
    request match {
      case KMGetActiveClusters =>
        sender ! KMQueryResult(clusterConfigMap.values.filter(_.enabled).toIndexedSeq)

      case KMGetAllClusters =>
        sender ! KMClusterList(clusterConfigMap.values.toIndexedSeq, pendingClusterConfigMap.values.toIndexedSeq)

      case KSGetScheduleLeaderElection =>
        sender ! ZkUtils.readDataMaybeNull(curator, ZkUtils.SchedulePreferredLeaderElectionPath)._1.getOrElse("{}")

      case KMGetClusterConfig(name) =>
        sender ! KMClusterConfigResult(Try {
          val cc = clusterConfigMap.get(name)
          require(cc.isDefined, s"Unknown cluster : $name")
          cc.get
        })

      case KMClusterQueryRequest(clusterName, request) =>
        clusterManagerMap.get(clusterName).fold[Unit] {
          sender ! ActorErrorResponse(s"Unknown cluster : $clusterName")
        } {
          clusterManagerPath:ActorPath =>
            context.actorSelection(clusterManagerPath).forward(request)
        }
        
      case any: Any => log.warning("kma : processQueryRequest : Received unknown message: {}", any)
    }
    
  }

  override def processCommandRequest(request: CommandRequest): Unit = {
    request match {
      case KMAddCluster(clusterConfig) =>
        modify {
          val data: Array[Byte] = ClusterConfig.serialize(clusterConfig)
          val zkpath: String = getConfigsZkPath(clusterConfig)
          require(!(clusterConfig.displaySizeEnabled && !clusterConfig.jmxEnabled),
            "Display topic and broker size can only be enabled when JMX is enabled")
          require(!(clusterConfig.filterConsumers && !clusterConfig.pollConsumers),
            "Filter consumers can only be enabled when consumer polling is enabled")
          require(kafkaManagerPathCache.getCurrentData(zkpath) == null,
            s"Cluster already exists : ${clusterConfig.name}")
          require(deleteClustersPathCache.getCurrentData(getDeleteClusterZkPath(clusterConfig.name)) == null,
            s"Cluster is marked for deletion : ${clusterConfig.name}")
          log.debug(s"Creating new config node $zkpath")
          curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(zkpath, data)
        }

      case KMUpdateCluster(clusterConfig) =>
        modify {
          val data: Array[Byte] = ClusterConfig.serialize(clusterConfig)
          val zkpath: String = getConfigsZkPath(clusterConfig)
          require(!(clusterConfig.displaySizeEnabled && !clusterConfig.jmxEnabled),
            "Display topic and broker size can only be enabled when JMX is enabled")
          require(!(clusterConfig.filterConsumers && !clusterConfig.pollConsumers),
            "Filter consumers can only be enabled when consumer polling is enabled")
          require(deleteClustersPathCache.getCurrentData(getDeleteClusterZkPath(clusterConfig.name)) == null,
            s"Cluster is marked for deletion : ${clusterConfig.name}")
          require(kafkaManagerPathCache.getCurrentData(zkpath) != null,
            s"Cannot update non-existing cluster : ${clusterConfig.name}")
          curator.setData().forPath(zkpath, data)
        }

      case KMDisableCluster(clusterName) =>
        modify {

          val existingConfigOption = clusterConfigMap.get(clusterName)
          require(existingConfigOption.isDefined, s"Cannot disable non-existing cluster : $clusterName")

          require(deleteClustersPathCache.getCurrentData(getDeleteClusterZkPath(clusterName)) == null,
            s"Cluster is marked for deletion : $clusterName")

          for {
            existingConfig <- existingConfigOption
          } yield {
            val disabledConfig = existingConfig.copy(enabled = false)
            val data: Array[Byte] = ClusterConfig.serialize(disabledConfig)
            val zkpath = getConfigsZkPath(existingConfig)
            require(kafkaManagerPathCache.getCurrentData(zkpath) != null,
              s"Cannot disable non-existing cluster : $clusterName")
            curator.setData().forPath(zkpath, data)
          }
        }

      case KMEnableCluster(clusterName) =>
        modify {
          val existingManagerOption = clusterManagerMap.get(clusterName)
          require(existingManagerOption.isEmpty, s"Cannot enable already enabled cluster : $clusterName")

          val existingConfigOption = clusterConfigMap.get(clusterName)
          require(existingConfigOption.isDefined, s"Cannot enable non-existing cluster : $clusterName")

          require(deleteClustersPathCache.getCurrentData(getDeleteClusterZkPath(clusterName)) == null,
            s"Cluster is marked for deletion : $clusterName")

          for {
            existingConfig <- existingConfigOption
          } yield {
            val enabledConfig = existingConfig.copy(enabled = true)
            val data: Array[Byte] = ClusterConfig.serialize(enabledConfig)
            val zkpath = getConfigsZkPath(existingConfig)
            require(kafkaManagerPathCache.getCurrentData(zkpath) != null,
              s"Cannot enable non-existing cluster : $clusterName")
            curator.setData().forPath(zkpath, data)
          }
        }

      case KMDeleteCluster(clusterName) =>
        modify {
          val existingManagerOption = clusterManagerMap.get(clusterName)
          require(existingManagerOption.isEmpty, s"Cannot delete enabled cluster : $clusterName")

          val existingConfigOption = clusterConfigMap.get(clusterName)
          require(existingConfigOption.isDefined, s"Cannot delete non-existing cluster : $clusterName")

          require(existingConfigOption.exists(!_.enabled), s"Cannot delete enabled cluster : $clusterName")

          for {
            existingConfig <- existingConfigOption
          } yield {
            val zkpath = getConfigsZkPath(existingConfig)
            require(kafkaManagerPathCache.getCurrentData(zkpath) != null,
              s"Cannot delete non-existing cluster : $clusterName")

            //mark for deletion
            val deleteZkPath = getDeleteClusterZkPath(existingConfig.name)
            curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(deleteZkPath)
          }
        }

      case KMClusterCommandRequest(clusterName, request) =>
        clusterManagerMap.get(clusterName).fold[Unit] {
          sender ! ActorErrorResponse(s"Unknown cluster : $clusterName")
        } {
          clusterManagerPath:ActorPath =>
            context.actorSelection(clusterManagerPath).forward(request)
        }

      case KMUpdateState =>
        updateState()

      case KMPruneClusters =>
        pruneClusters()

      case KMShutdown =>
        log.info(s"Shutting down kafka manager")
        context.children.foreach(context.stop)
        shutdown = true

      case any: Any => log.warning("kma : processCommandRequest : Received unknown message: {}", any)
    }
  }

  private[this] def getDeleteClusterZkPath(clusterName: String) : String = {
    zkPathFrom(deleteClustersZkPath,clusterName)
  }

  private[this] def getConfigsZkPath(clusterConfig: ClusterConfig) : String = {
    zkPathFrom(configsZkPath,clusterConfig.name)
  }

  private[this] def getClusterZkPath(clusterConfig: ClusterConfig) : String = {
    zkPathFrom(baseClusterZkPath,clusterConfig.name)
  }

  private[this] def markPendingClusterManager(clusterConfig: ClusterConfig) : Unit = {
    implicit val ec = context.system.dispatcher
    log.info(s"Mark pending cluster manager $clusterConfig")
    pendingClusterConfigMap += (clusterConfig.name -> clusterConfig)
  }

  private[this] def removeClusterManager(clusterConfig: ClusterConfig) : Unit = {
    implicit val ec = context.system.dispatcher
    clusterManagerMap.get(clusterConfig.name).foreach { actorPath =>
      log.info(s"Removing cluster manager $clusterConfig")
      val selection = context.actorSelection(actorPath)
      selection.tell(CMShutdown,self)

      //this is non-blocking
      selection.resolveOne(1 seconds).foreach( ref => context.stop(ref) )
    }
    clusterManagerMap -= clusterConfig.name
    clusterConfigMap -= clusterConfig.name
  }

  private[this] def getConfigWithDefaults(config: ClusterConfig, kmConfig: KafkaManagerActorConfig) : ClusterConfig = {
    val brokerViewUpdatePeriodSeconds = config.tuning.flatMap(_.brokerViewUpdatePeriodSeconds) orElse kmConfig.defaultTuning.brokerViewUpdatePeriodSeconds
    val clusterManagerThreadPoolSize = config.tuning.flatMap(_.clusterManagerThreadPoolSize) orElse kmConfig.defaultTuning.clusterManagerThreadPoolSize
    val clusterManagerThreadPoolQueueSize = config.tuning.flatMap(_.clusterManagerThreadPoolQueueSize) orElse kmConfig.defaultTuning.clusterManagerThreadPoolQueueSize
    val kafkaCommandThreadPoolSize = config.tuning.flatMap(_.kafkaCommandThreadPoolSize) orElse kmConfig.defaultTuning.kafkaCommandThreadPoolSize
    val kafkaCommandThreadPoolQueueSize = config.tuning.flatMap(_.kafkaCommandThreadPoolQueueSize) orElse kmConfig.defaultTuning.kafkaCommandThreadPoolQueueSize
    val logkafkaCommandThreadPoolSize = config.tuning.flatMap(_.logkafkaCommandThreadPoolSize) orElse kmConfig.defaultTuning.logkafkaCommandThreadPoolSize
    val logkafkaCommandThreadPoolQueueSize = config.tuning.flatMap(_.logkafkaCommandThreadPoolQueueSize) orElse kmConfig.defaultTuning.logkafkaCommandThreadPoolQueueSize
    val logkafkaUpdatePeriodSeconds = config.tuning.flatMap(_.logkafkaUpdatePeriodSeconds) orElse kmConfig.defaultTuning.brokerViewUpdatePeriodSeconds
    val partitionOffsetCacheTimeoutSecs = config.tuning.flatMap(_.partitionOffsetCacheTimeoutSecs) orElse kmConfig.defaultTuning.partitionOffsetCacheTimeoutSecs
    val brokerViewThreadPoolSize = config.tuning.flatMap(_.brokerViewThreadPoolSize) orElse kmConfig.defaultTuning.brokerViewThreadPoolSize
    val brokerViewThreadPoolQueueSize = config.tuning.flatMap(_.brokerViewThreadPoolQueueSize) orElse kmConfig.defaultTuning.brokerViewThreadPoolQueueSize
    val offsetCacheThreadPoolSize = config.tuning.flatMap(_.offsetCacheThreadPoolSize) orElse kmConfig.defaultTuning.offsetCacheThreadPoolSize
    val offsetCacheThreadPoolQueueSize = config.tuning.flatMap(_.offsetCacheThreadPoolQueueSize) orElse kmConfig.defaultTuning.offsetCacheThreadPoolQueueSize
    val kafkaAdminClientThreadPoolSize = config.tuning.flatMap(_.kafkaAdminClientThreadPoolSize) orElse kmConfig.defaultTuning.kafkaAdminClientThreadPoolSize
    val kafkaAdminClientThreadPoolQueueSize = config.tuning.flatMap(_.kafkaAdminClientThreadPoolQueueSize) orElse kmConfig.defaultTuning.kafkaAdminClientThreadPoolQueueSize
    val kafkaManagedOffsetMetadataCheckMillis = config.tuning.flatMap(_.kafkaManagedOffsetMetadataCheckMillis) orElse kmConfig.defaultTuning.kafkaManagedOffsetMetadataCheckMillis
    val kafkaManagedOffsetGroupCacheSize = config.tuning.flatMap(_.kafkaManagedOffsetGroupCacheSize) orElse kmConfig.defaultTuning.kafkaManagedOffsetGroupCacheSize
    val kafkaManagedOffsetGroupExpireDays = config.tuning.flatMap(_.kafkaManagedOffsetGroupExpireDays) orElse kmConfig.defaultTuning.kafkaManagedOffsetGroupExpireDays

    val tuning = Option(
      ClusterTuning(
        brokerViewUpdatePeriodSeconds = brokerViewUpdatePeriodSeconds
        , clusterManagerThreadPoolSize = clusterManagerThreadPoolSize
        , clusterManagerThreadPoolQueueSize = clusterManagerThreadPoolQueueSize
        , kafkaCommandThreadPoolSize = kafkaCommandThreadPoolSize
        , kafkaCommandThreadPoolQueueSize = kafkaCommandThreadPoolQueueSize
        , logkafkaCommandThreadPoolSize = logkafkaCommandThreadPoolSize
        , logkafkaCommandThreadPoolQueueSize = logkafkaCommandThreadPoolQueueSize
        , logkafkaUpdatePeriodSeconds = logkafkaUpdatePeriodSeconds
        , partitionOffsetCacheTimeoutSecs = partitionOffsetCacheTimeoutSecs
        , brokerViewThreadPoolSize = brokerViewThreadPoolSize
        , brokerViewThreadPoolQueueSize = brokerViewThreadPoolQueueSize
        , offsetCacheThreadPoolSize = offsetCacheThreadPoolSize
        , offsetCacheThreadPoolQueueSize = offsetCacheThreadPoolQueueSize
        , kafkaAdminClientThreadPoolSize = kafkaAdminClientThreadPoolSize
        , kafkaAdminClientThreadPoolQueueSize = kafkaAdminClientThreadPoolQueueSize
        , kafkaManagedOffsetMetadataCheckMillis = kafkaManagedOffsetMetadataCheckMillis
        , kafkaManagedOffsetGroupCacheSize = kafkaManagedOffsetGroupCacheSize
        , kafkaManagedOffsetGroupExpireDays = kafkaManagedOffsetGroupExpireDays
      )
    )
    config.copy(
      tuning = tuning
    )
  }
  private[this] def addCluster(config: ClusterConfig): Try[Boolean] = {
    Try {
      if(!config.enabled) {
        log.info("Not adding cluster manager for disabled cluster : {}", config.name)
        clusterConfigMap += (config.name -> config)
        pendingClusterConfigMap -= config.name
        false
      } else {
        log.info("Adding new cluster manager for cluster : {}", config.name)
        val clusterManagerConfig = ClusterManagerActorConfig(
          kafkaManagerConfig.pinnedDispatcherName
          , getClusterZkPath(config)
          , kafkaManagerConfig.curatorConfig
          , config
          , askTimeoutMillis = kafkaManagerConfig.clusterActorsAskTimeoutMillis
          , mutexTimeoutMillis = kafkaManagerConfig.mutexTimeoutMillis
          , simpleConsumerSocketTimeoutMillis = kafkaManagerConfig.simpleConsumerSocketTimeoutMillis
          , consumerProperties = kafkaManagerConfig.consumerProperties
        )
        val props = Props(classOf[ClusterManagerActor], clusterManagerConfig)
        val newClusterManager = context.actorOf(props, config.name).path
        clusterConfigMap += (config.name -> config)
        clusterManagerMap += (config.name -> newClusterManager)
        pendingClusterConfigMap -= config.name
        true
      }
    }
  }

  private[this] def updateCluster(currentConfig: ClusterConfig, newConfig: ClusterConfig): Try[Boolean] = {
    Try {
      if(newConfig.curatorConfig.zkConnect == currentConfig.curatorConfig.zkConnect
        && newConfig.enabled == currentConfig.enabled
        && newConfig.version == currentConfig.version
        && newConfig.jmxEnabled == currentConfig.jmxEnabled
        && newConfig.jmxUser == currentConfig.jmxUser
        && newConfig.jmxPass == currentConfig.jmxPass
        && newConfig.jmxSsl == currentConfig.jmxSsl
        && newConfig.logkafkaEnabled == currentConfig.logkafkaEnabled
        && newConfig.pollConsumers == currentConfig.pollConsumers
        && newConfig.filterConsumers == currentConfig.filterConsumers
        && newConfig.activeOffsetCacheEnabled == currentConfig.activeOffsetCacheEnabled
        && newConfig.displaySizeEnabled == currentConfig.displaySizeEnabled
        && newConfig.tuning == currentConfig.tuning
        && newConfig.securityProtocol == currentConfig.securityProtocol
        && newConfig.saslMechanism == currentConfig.saslMechanism
        && newConfig.jaasConfig == currentConfig.jaasConfig
      ) {
        //nothing changed
        false
      } else {
        //only need to shutdown enabled cluster
        log.info("Updating cluster manager for cluster={} , old={}, new={}",
          currentConfig.name,currentConfig,newConfig)
        markPendingClusterManager(newConfig)
        removeClusterManager(currentConfig)
        true
      }
    }
  }

  private[this] def updateState(): Unit = {
    log.info("Updating internal state...")
    val result = Try {
      kafkaManagerPathCache.getCurrentData.asScala.foreach { data =>
        ClusterConfig.deserialize(data.getData) match {
          case Failure(t) =>
            log.error("Failed to deserialize cluster config",t)
          case Success(newConfig) =>
            val configWithDefaults = getConfigWithDefaults(newConfig, kafkaManagerConfig)
            clusterConfigMap.get(newConfig.name).fold(addCluster(configWithDefaults))(updateCluster(_,configWithDefaults))
        }
      }
    }
    result match {
      case Failure(t) =>
        log.error("Failed to update internal state ... ",t)
      case _ =>
    }
    lastUpdateMillis = System.currentTimeMillis()
  }

  private[this] def pruneClusters(): Unit = {
    log.info("Pruning clusters...")
    Try {
      val localClusterConfigMap = clusterConfigMap
      localClusterConfigMap.foreach { case (name, clusterConfig) =>
        val zkpath : String = getConfigsZkPath(clusterConfig)
        if(kafkaManagerPathCache.getCurrentData(zkpath) == null) {
          pendingClusterConfigMap -= clusterConfig.name
          removeClusterManager(clusterConfig)
          clusterConfigMap -= name
        }
      }
    }
    lastUpdateMillis = System.currentTimeMillis()
  }
}
