/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import java.nio.charset.StandardCharsets
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit, ThreadPoolExecutor}

import akka.pattern._
import akka.actor.{Props, ActorPath}
import ActorModel.CMShutdown
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener, PathChildrenCache}
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.zookeeper.CreateMode

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Success, Failure, Try}
import scala.util.matching.Regex

/**
 * @author hiral
 */

sealed trait KafkaVersion
case object Kafka_0_8_1_1 extends KafkaVersion {
  override def toString = "0.8.1.1"
}
case object Kafka_0_8_2_beta extends KafkaVersion {
  override def toString = "0.8.2-beta"
}

object KafkaVersion {
  val supportedVersions: Map[String,KafkaVersion] = Map("0.8.1.1" -> Kafka_0_8_1_1, "0.8.2-beta" -> Kafka_0_8_2_beta)

  val formSelectList : IndexedSeq[(String,String)] = supportedVersions.toIndexedSeq.map(t => (t._1,t._2.toString))

  def apply(s: String) : KafkaVersion = {
    supportedVersions.get(s) match {
      case Some(v) => v
      case None => throw new IllegalArgumentException(s"Unsupported kafka version : $s")
    }
  }

  def unapply(v: KafkaVersion) : Option[String] = {
    Some(v.toString)
  }
}

object ClusterConfig {
  val legalChars = "[a-zA-Z0-9\\._\\-]"
  private val maxNameLength = 255
  val regex = new Regex(legalChars + "+")

  def validateName(clusterName: String) {
    require(clusterName.length > 0, "cluster name is illegal, can't be empty")
    require(!(clusterName.equals(".") || clusterName.equals("..")), "cluster name cannot be \".\" or \"..\"")
    require(clusterName.length <= maxNameLength,"cluster name is illegal, can't be longer than " + maxNameLength + " characters")
    regex.findFirstIn(clusterName) match {
      case Some(t) =>
        require(t.equals(clusterName),
          ("cluster name " + clusterName + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'"))
      case None =>
        require(false,"cluster name " + clusterName + " is illegal,  contains a character other than ASCII alphanumerics, '.', '_' and '-'")
    }
  }

  def validateZkHosts(zkHosts: String): Unit = {
    require(zkHosts.length > 0, "cluster zk hosts is illegal, can't be empty!")
    require(zkHosts.contains(":"), "cluster zk hosts is illegal, no port defined!")
  }

  def apply(name: String, version : String, zkHosts: String, zkMaxRetry: Int = 100) : ClusterConfig = {
    val kafkaVersion = KafkaVersion(version)
    //validate cluster name
    validateName(name)
    //validate zk hosts
    validateZkHosts(zkHosts)
    val cleanZkHosts = zkHosts.replaceAll(" ","").toLowerCase
    new ClusterConfig(name.toLowerCase, CuratorConfig(cleanZkHosts, zkMaxRetry), true, kafkaVersion)
  }

  def customUnapply(cc: ClusterConfig) : Option[(String, String, String, Int)] = {
    Some((cc.name, cc.version.toString, cc.curatorConfig.zkConnect, cc.curatorConfig.zkMaxRetry))
  }

  import scalaz.{Failure,Success}
  import scalaz.syntax.applicative._
  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  import org.json4s.jackson.Serialization
  import org.json4s.scalaz.JsonScalaz._
  import scala.language.reflectiveCalls

  implicit val formats = Serialization.formats(FullTypeHints(List(classOf[ClusterConfig])))

  implicit def curatorConfigJSONW: JSONW[CuratorConfig] = new JSONW[CuratorConfig] {
    def write(a: CuratorConfig) =
      makeObj(("zkConnect" -> toJSON(a.zkConnect))
        :: ("zkMaxRetry" -> toJSON(a.zkMaxRetry))
        :: ("baseSleepTimeMs" -> toJSON(a.baseSleepTimeMs))
        :: ("maxSleepTimeMs" -> toJSON(a.maxSleepTimeMs))
        :: Nil)
  }

  implicit def curatorConfigJSONR: JSONR[CuratorConfig] = CuratorConfig.applyJSON(
    field[String]("zkConnect"), field[Int]("zkMaxRetry"), field[Int]("baseSleepTimeMs"), field[Int]("maxSleepTimeMs"))

  def serialize(config: ClusterConfig) : Array[Byte] = {
    val json = makeObj(("name" -> toJSON(config.name))
      :: ("curatorConfig" -> toJSON(config.curatorConfig))
      :: ("enabled" -> toJSON(config.enabled))
      :: ("kafkaVersion" -> toJSON(config.version.toString))
      :: Nil)
    compact(render(json)).getBytes(StandardCharsets.UTF_8)
  }

  def deserialize(ba: Array[Byte]) : Try[ClusterConfig] = {
    Try {
      val json = parse(kafka.manager.utils.deserializeString(ba))

      val result = (field[String]("name")(json) |@| field[CuratorConfig]("curatorConfig")(json) |@| field[Boolean]("enabled")(json))
      {
        (name:String,curatorConfig:CuratorConfig,enabled:Boolean) =>
          val versionString = field[String]("kafkaVersion")(json)
          val version = versionString.map(KafkaVersion.apply).getOrElse(Kafka_0_8_1_1)
          ClusterConfig.apply(name,curatorConfig,enabled,version)
      }

      result match {
        case Failure(nel) =>
          throw new IllegalArgumentException(nel.toString())
        case Success(clusterConfig) =>
          clusterConfig
      }

    }
  }

}

case class ClusterConfig (name: String, curatorConfig : CuratorConfig, enabled: Boolean, version: KafkaVersion)

object KafkaManagerActor {
  val ZkRoot : String = "/kafka-manager"

  def getClusterPath(config: ClusterConfig) : String = s"$ZkRoot/${config.name}"

}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import ActorModel._

case class KafkaManagerActorConfig(curatorConfig: CuratorConfig,
                                   baseZkPath : String = KafkaManagerActor.ZkRoot,
                                   pinnedDispatcherName : String = "pinned-dispatcher",
                                   brokerViewUpdatePeriod: FiniteDuration = 10 seconds,
                                   startDelayMillis: Long = 1000,
                                   threadPoolSize: Int = 2,
                                   mutexTimeoutMillis: Int = 4000,
                                   maxQueueSize: Int = 100,
                                   kafkaManagerUpdatePeriod: FiniteDuration = 10 seconds,
                                   deleteClusterUpdatePeriod: FiniteDuration = 10 seconds,
                                   deletionBatchSize : Int = 2)
class KafkaManagerActor(kafkaManagerConfig: KafkaManagerActorConfig)
  extends CuratorAwareActor(kafkaManagerConfig.curatorConfig) with BaseZkPath {

  //this is for baze zk path trait
  override val baseZkPath : String = kafkaManagerConfig.baseZkPath

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
          mutex.acquire(kafkaManagerConfig.mutexTimeoutMillis,TimeUnit.MILLISECONDS)
          KMCommandResult(Try {
            fn
          })
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
    context.system.scheduler.schedule(
      Duration(kafkaManagerConfig.startDelayMillis,TimeUnit.MILLISECONDS),kafkaManagerConfig.kafkaManagerUpdatePeriod) {
      self ! KMUpdateState
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
      case any: Any => log.warning("Received unknown message: {}", any)
    }
  }

  override def processActorRequest(request: ActorRequest): Unit = {
    request match {
      case KMGetActiveClusters =>
        sender ! KMQueryResult(clusterConfigMap.values.filter(_.enabled).toIndexedSeq)

      case KMGetAllClusters =>
        sender ! KMClusterList(clusterConfigMap.values.toIndexedSeq,pendingClusterConfigMap.values.toIndexedSeq)

      case KMGetClusterConfig(name) =>
        sender ! KMClusterConfigResult( Try {
          val cc = clusterConfigMap.get(name)
          require(cc.isDefined, s"Unknown cluster : $name")
          cc.get
        })

      case KMAddCluster(clusterConfig) =>
        modify {
          val data: Array[Byte] = ClusterConfig.serialize(clusterConfig)
          val zkpath: String = getConfigsZkPath(clusterConfig)
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

      case KMClusterQueryRequest(clusterName, request) =>
        clusterManagerMap.get(clusterName).fold[Unit] {
          sender ! ActorErrorResponse(s"Unknown cluster : $clusterName")
        } {
          clusterManagerPath:ActorPath =>
            context.actorSelection(clusterManagerPath).forward(request)
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

      case any: Any => log.warning("Received unknown message: {}", any)
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
          kafkaManagerConfig.pinnedDispatcherName,
          getClusterZkPath(config),
          kafkaManagerConfig.curatorConfig,
          config,
          kafkaManagerConfig.brokerViewUpdatePeriod)
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
        && newConfig.version == currentConfig.version) {
        //nothing changed
        false
      } else {
        //only need to shutdown enabled cluster
        log.info("Updating cluster manager for cluster={} , old={}, new={}",
          currentConfig.name,currentConfig.curatorConfig,newConfig.curatorConfig)
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
            clusterConfigMap.get(newConfig.name).fold(addCluster(newConfig))(updateCluster(_,newConfig))
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
