/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import akka.actor.{ActorPath, ActorSystem, Cancellable, Props}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import grizzled.slf4j.Logging
import kafka.manager.actor.cluster.KafkaManagedOffsetCacheConfig
import kafka.manager.actor.{KafkaManagerActor, KafkaManagerActorConfig}
import kafka.manager.model.ActorModel._
import kafka.manager.model._
import kafka.manager.utils.UtilException
import kafka.manager.utils.zero81.ReassignPartitionErrors.ReplicationOutOfSync
import kafka.manager.utils.zero81.{ForceOnReplicationOutOfSync, ForceReassignmentCommand}
import org.json4s.jackson.JsonMethods.parse

import java.util.Properties
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/**
 * @author hiral
 */
case class TopicListExtended(list: IndexedSeq[(String, Option[TopicIdentity])],
                             topicToConsumerMap: Map[String, Iterable[(String, ConsumerType)]],
                             deleteSet: Set[String],
                             underReassignments: IndexedSeq[String],
                             clusterContext: ClusterContext)
case class BrokerListExtended(list: IndexedSeq[BrokerIdentity],
                              metrics: Map[Int,BrokerMetrics],
                              combinedMetric: Option[BrokerMetrics],
                              clusterContext: ClusterContext)
case class ConsumerListExtended(list: IndexedSeq[((String, ConsumerType), Option[ConsumerIdentity])], clusterContext: ClusterContext)
case class LogkafkaListExtended(list: IndexedSeq[(String, Option[LogkafkaIdentity])], deleteSet: Set[String])

case class ApiError(msg: String, recoverByForceOperation: Boolean = false)
object ApiError extends Logging {

  implicit def fromThrowable(t: Throwable) : ApiError = {
    error(s"error : ${t.getMessage}", t)
    ApiError(t.getMessage)
  }

  implicit def from(actorError: ActorErrorResponse): ApiError = {
    actorError.throwableOption.foreach { t =>
      error(s"Actor error : ${actorError.msg}", t)
    }
    ApiError(actorError.msg)
  }
}

import akka.pattern._
import scalaz.{-\/, \/, \/-}
class KafkaManager(akkaConfig: Config) extends Logging {

  def getPrefixedKey(key: String): String = if (akkaConfig.hasPathOrNull(s"cmak.$key")) s"cmak.$key" else s"kafka-manager.$key"

  val ConsumerPropertiesFile = getPrefixedKey("consumer.properties.file")
  val BaseZkPath = getPrefixedKey("base-zk-path")
  val PinnedDispatchName = getPrefixedKey("pinned-dispatcher-name")
  val ZkHosts = getPrefixedKey("zkhosts")
  val BrokerViewUpdateSeconds = getPrefixedKey("broker-view-update-seconds")
  val KafkaManagerUpdateSeconds = getPrefixedKey("kafka-manager-update-seconds")
  val DeleteClusterUpdateSeconds = getPrefixedKey("delete-cluster-update-seconds")
  val DeletionBatchSize = getPrefixedKey("deletion-batch-size")
  val MaxQueueSize = getPrefixedKey("max-queue-size")
  val ThreadPoolSize = getPrefixedKey("thread-pool-size")
  val MutexTimeoutMillis = getPrefixedKey("mutex-timeout-millis")
  val StartDelayMillis = getPrefixedKey("start-delay-millis")
  val ApiTimeoutMillis = getPrefixedKey("api-timeout-millis")
  val ClusterActorsAskTimeoutMillis = getPrefixedKey("cluster-actors-ask-timeout-millis")
  val PartitionOffsetCacheTimeoutSecs = getPrefixedKey("partition-offset-cache-timeout-secs")
  val SimpleConsumerSocketTimeoutMillis = getPrefixedKey("simple-consumer-socket-timeout-millis")
  val BrokerViewThreadPoolSize = getPrefixedKey("broker-view-thread-pool-size")
  val BrokerViewMaxQueueSize = getPrefixedKey("broker-view-max-queue-size")
  val OffsetCacheThreadPoolSize = getPrefixedKey("offset-cache-thread-pool-size")
  val OffsetCacheMaxQueueSize = getPrefixedKey("offset-cache-max-queue-size")
  val KafkaAdminClientThreadPoolSize = getPrefixedKey("kafka-admin-client-thread-pool-size")
  val KafkaAdminClientMaxQueueSize = getPrefixedKey("kafka-admin-client-max-queue-size")
  val KafkaManagedOffsetMetadataCheckMillis = getPrefixedKey("kafka-managed-offset-metadata-check-millis")
  val KafkaManagedOffsetGroupCacheSize = getPrefixedKey("kafka-managed-offset-group-cache-size")
  val KafkaManagedOffsetGroupExpireDays = getPrefixedKey("kafka-managed-offset-group-expire-days")

  val DefaultConfig: Config = {
    val defaults: Map[String, _ <: AnyRef] = Map(
      BaseZkPath -> KafkaManagerActor.ZkRoot,
      PinnedDispatchName -> "pinned-dispatcher",
      BrokerViewUpdateSeconds -> "30",
      KafkaManagerUpdateSeconds -> "10",
      DeleteClusterUpdateSeconds -> "10",
      DeletionBatchSize -> "2",
      MaxQueueSize -> "100",
      ThreadPoolSize -> "2",
      MutexTimeoutMillis -> "4000",
      StartDelayMillis -> "1000",
      ApiTimeoutMillis -> "5000",
      ClusterActorsAskTimeoutMillis -> "2000",
      PartitionOffsetCacheTimeoutSecs -> "5",
      SimpleConsumerSocketTimeoutMillis -> "10000",
      BrokerViewThreadPoolSize -> Runtime.getRuntime.availableProcessors().toString,
      BrokerViewMaxQueueSize -> "1000",
      OffsetCacheThreadPoolSize -> Runtime.getRuntime.availableProcessors().toString,
      OffsetCacheMaxQueueSize -> "1000",
      KafkaAdminClientThreadPoolSize -> Runtime.getRuntime.availableProcessors().toString,
      KafkaAdminClientMaxQueueSize -> "1000",
      KafkaManagedOffsetMetadataCheckMillis -> KafkaManagedOffsetCacheConfig.defaultGroupMemberMetadataCheckMillis.toString,
      KafkaManagedOffsetGroupCacheSize -> KafkaManagedOffsetCacheConfig.defaultGroupTopicPartitionOffsetMaxSize.toString,
      KafkaManagedOffsetGroupExpireDays -> KafkaManagedOffsetCacheConfig.defaultGroupTopicPartitionOffsetExpireDays.toString
    )
    import scala.collection.JavaConverters._
    ConfigFactory.parseMap(defaults.asJava)
  }

  private[this] val system = ActorSystem("kafka-manager-system", akkaConfig)

  private[this] val configWithDefaults = akkaConfig.withFallback(DefaultConfig)
  val defaultTuning = ClusterTuning(
    brokerViewUpdatePeriodSeconds = Option(configWithDefaults.getInt(BrokerViewUpdateSeconds))
    , clusterManagerThreadPoolSize = Option(configWithDefaults.getInt(ThreadPoolSize))
    , clusterManagerThreadPoolQueueSize = Option(configWithDefaults.getInt(MaxQueueSize))
    , kafkaCommandThreadPoolSize = Option(configWithDefaults.getInt(ThreadPoolSize))
    , kafkaCommandThreadPoolQueueSize = Option(configWithDefaults.getInt(MaxQueueSize))
    , logkafkaCommandThreadPoolSize = Option(configWithDefaults.getInt(ThreadPoolSize))
    , logkafkaCommandThreadPoolQueueSize = Option(configWithDefaults.getInt(MaxQueueSize))
    , logkafkaUpdatePeriodSeconds = Option(configWithDefaults.getInt(BrokerViewUpdateSeconds))
    , partitionOffsetCacheTimeoutSecs = Option(configWithDefaults.getInt(PartitionOffsetCacheTimeoutSecs))
    , brokerViewThreadPoolSize = Option(configWithDefaults.getInt(BrokerViewThreadPoolSize))
    , brokerViewThreadPoolQueueSize = Option(configWithDefaults.getInt(BrokerViewMaxQueueSize))
    , offsetCacheThreadPoolSize = Option(configWithDefaults.getInt(OffsetCacheThreadPoolSize))
    , offsetCacheThreadPoolQueueSize = Option(configWithDefaults.getInt(OffsetCacheMaxQueueSize))
    , kafkaAdminClientThreadPoolSize = Option(configWithDefaults.getInt(KafkaAdminClientThreadPoolSize))
    , kafkaAdminClientThreadPoolQueueSize = Option(configWithDefaults.getInt(KafkaAdminClientMaxQueueSize))
    , kafkaManagedOffsetMetadataCheckMillis = Option(configWithDefaults.getInt(KafkaManagedOffsetMetadataCheckMillis))
    , kafkaManagedOffsetGroupCacheSize = Option(configWithDefaults.getInt(KafkaManagedOffsetGroupCacheSize))
    , kafkaManagedOffsetGroupExpireDays = Option(configWithDefaults.getInt(KafkaManagedOffsetGroupExpireDays))
  )
  private[this] val kafkaManagerConfig = {
    val curatorConfig = CuratorConfig(configWithDefaults.getString(ZkHosts))
    KafkaManagerActorConfig(
      curatorConfig = curatorConfig
      , baseZkPath = configWithDefaults.getString(BaseZkPath)
      , pinnedDispatcherName = configWithDefaults.getString(PinnedDispatchName)
      , startDelayMillis = configWithDefaults.getLong(StartDelayMillis)
      , threadPoolSize = configWithDefaults.getInt(ThreadPoolSize)
      , mutexTimeoutMillis = configWithDefaults.getInt(MutexTimeoutMillis)
      , maxQueueSize = configWithDefaults.getInt(MaxQueueSize)
      , kafkaManagerUpdatePeriod = FiniteDuration(configWithDefaults.getInt(KafkaManagerUpdateSeconds), SECONDS)
      , deleteClusterUpdatePeriod = FiniteDuration(configWithDefaults.getInt(DeleteClusterUpdateSeconds), SECONDS)
      , deletionBatchSize = configWithDefaults.getInt(DeletionBatchSize)
      , clusterActorsAskTimeoutMillis = configWithDefaults.getInt(ClusterActorsAskTimeoutMillis)
      , simpleConsumerSocketTimeoutMillis =  configWithDefaults.getInt(SimpleConsumerSocketTimeoutMillis)
      , defaultTuning = defaultTuning
      , consumerProperties = getConsumerPropertiesFromConfig(configWithDefaults)
    )
  }

  private[this] val props = Props(classOf[KafkaManagerActor], kafkaManagerConfig)

  private[this] val kafkaManagerActor: ActorPath = system.actorOf(props, "kafka-manager").path

  private[this] val apiExecutor = new ThreadPoolExecutor(
    kafkaManagerConfig.threadPoolSize,
    kafkaManagerConfig.threadPoolSize,
    0L,
    TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable](kafkaManagerConfig.maxQueueSize)
  )

  private[this] val apiExecutionContext = ExecutionContext.fromExecutor(apiExecutor)

  private[this] implicit val apiTimeout: Timeout = FiniteDuration(
    configWithDefaults.getInt(ApiTimeoutMillis),
    MILLISECONDS
  )

  private[this] def getConsumerPropertiesFromConfig(config: Config) : Option[Properties] = {
    if(config.hasPath(ConsumerPropertiesFile)) {
      val filePath = config.getString(ConsumerPropertiesFile)
      val file = new java.io.File(filePath)
      if(file.isFile & file.canRead) {
        val props = new Properties()
        props.load(new java.io.FileInputStream(file))
        return Option(props)
      } else {
        warn(s"Failed to find consumer properties file or file is not readable : $file")
      }
    }
    None
  }

  private[this] def tryWithKafkaManagerActor[Input, Output, FOutput](msg: Input)
    (fn: Output => FOutput)
    (implicit tag: ClassTag[Output]): Future[ApiError \/ FOutput] =
  {
    implicit val ec = apiExecutionContext
    system.actorSelection(kafkaManagerActor).ask(msg).map {
      case err: ActorErrorResponse => 
        error(s"Failed on input : $msg")
        -\/(ApiError.from(err))
      case o: Output =>
        Try {
          fn(o)
        } match {
          case Failure(t) => 
            error(s"Failed on input : $msg")
            -\/(ApiError.fromThrowable(t))
          case Success(foutput) => \/-(foutput)
        }
    }.recover { case t: Throwable =>
      error(s"Failed on input : $msg", t)
      -\/(ApiError.fromThrowable(t))
    }
  }

  private[this] def withKafkaManagerActor[Input, Output, FOutput](msg: Input)
    (fn: Output => Future[ApiError \/ FOutput])
    (implicit tag: ClassTag[Output]): Future[ApiError \/ FOutput] =
  {
    implicit val ec = apiExecutionContext
    system.actorSelection(kafkaManagerActor).ask(msg).flatMap {
      case err: ActorErrorResponse => Future.successful(-\/(ApiError.from(err)))
      case o: Output =>
        fn(o)
    }.recover { case t: Throwable =>
      -\/(ApiError.fromThrowable(t))
    }
  }

  private[this] def toDisjunction[T](t: Try[T]): ApiError \/ T = {
    t match {
      case Failure(th) =>
        -\/(th)
      case Success(tInst) =>
        \/-(tInst)
    }
  }

  def shutdown(): Unit = {
    implicit val ec = apiExecutionContext
    system.actorSelection(kafkaManagerActor).tell(KMShutdown, system.deadLetters)
    Try(Await.ready(system.terminate(), Duration(30, TimeUnit.SECONDS)))
    apiExecutor.shutdown()
  }

  //--------------------Commands--------------------------
  def addCluster(clusterName: String,
                 version: String,
                 zkHosts: String,
                 jmxEnabled: Boolean,
                 jmxUser: Option[String],
                 jmxPass: Option[String],
                 jmxSsl: Boolean,
                 pollConsumers: Boolean,
                 filterConsumers: Boolean,
                 tuning: Option[ClusterTuning],
                 securityProtocol: String,
                 saslMechanism: Option[String],
                 jaasConfig: Option[String],
                 logkafkaEnabled: Boolean = false,
                 activeOffsetCacheEnabled: Boolean = false,
                 displaySizeEnabled: Boolean = false): Future[ApiError \/ Unit] =
  {
    val cc = ClusterConfig(
      clusterName,
      version,
      zkHosts,
      tuning = tuning,
      securityProtocol = securityProtocol,
      saslMechanism = saslMechanism,
      jaasConfig = jaasConfig,
      jmxEnabled = jmxEnabled,
      jmxUser = jmxUser,
      jmxPass = jmxPass,
      jmxSsl = jmxSsl,
      pollConsumers = pollConsumers,
      filterConsumers = filterConsumers,
      logkafkaEnabled = logkafkaEnabled,
      activeOffsetCacheEnabled = activeOffsetCacheEnabled,
      displaySizeEnabled = displaySizeEnabled)
    tryWithKafkaManagerActor(KMAddCluster(cc)) { result: KMCommandResult =>
      result.result.get
    }
  }

  def updateCluster(clusterName: String,
                    version: String,
                    zkHosts: String,
                    jmxEnabled: Boolean,
                    jmxUser: Option[String],
                    jmxPass: Option[String],
                    jmxSsl: Boolean,
                    pollConsumers: Boolean,
                    filterConsumers: Boolean,
                    tuning: Option[ClusterTuning],
                    securityProtocol: String,
                    saslMechanism: Option[String],
                    jaasConfig: Option[String],
                    logkafkaEnabled: Boolean = false,
                    activeOffsetCacheEnabled: Boolean = false,
                    displaySizeEnabled: Boolean = false): Future[ApiError \/ Unit] =
  {
    val cc = ClusterConfig(
      clusterName,
      version,
      zkHosts,
      tuning = tuning,
      securityProtocol = securityProtocol,
      saslMechanism = saslMechanism,
      jaasConfig = jaasConfig,
      jmxEnabled = jmxEnabled,
      jmxUser = jmxUser,
      jmxPass = jmxPass,
      jmxSsl = jmxSsl,
      pollConsumers = pollConsumers,
      filterConsumers = filterConsumers,
      logkafkaEnabled = logkafkaEnabled,
      activeOffsetCacheEnabled = activeOffsetCacheEnabled,
      displaySizeEnabled = displaySizeEnabled)
    tryWithKafkaManagerActor(KMUpdateCluster(cc)) { result: KMCommandResult =>
      result.result.get
    }
  }

  def disableCluster(clusterName: String): Future[ApiError \/ Unit] = {
    tryWithKafkaManagerActor(KMDisableCluster(clusterName)) { result: KMCommandResult =>
      result.result.get
    }
  }

  def enableCluster(clusterName: String): Future[ApiError \/ Unit] = {
    tryWithKafkaManagerActor(KMEnableCluster(clusterName)) { result: KMCommandResult =>
      result.result.get
    }
  }

  def deleteCluster(clusterName: String): Future[ApiError \/ Unit] = {
    tryWithKafkaManagerActor(KMDeleteCluster(clusterName)) { result: KMCommandResult =>
      result.result.get
    }
  }

  def runPreferredLeaderElection(clusterName: String, topics: Set[String]): Future[ApiError \/ ClusterContext] = {
    implicit val ec = apiExecutionContext
    withKafkaManagerActor(
      KMClusterCommandRequest(
        clusterName,
        CMRunPreferredLeaderElection(topics)
      )
    ) { result: Future[CMCommandResult] =>
      result.map(cmr => toDisjunction(cmr.result))
    }
  }

  private def runPreferredLeaderElectionWithAllTopics(clusterName: String) = {
    implicit val ec = apiExecutionContext

    getTopicList(clusterName).flatMap { errorOrTopicList =>
      errorOrTopicList.fold({ e =>
        Future.successful(-\/(e))
      }, { topicList =>
        runPreferredLeaderElection(clusterName, topicList.list.toSet)
      })
    }
  }

  private def updateSchedulePreferredLeaderElection(clusterName: String): Unit = {
    system.actorSelection(kafkaManagerActor).ask(KMClusterCommandRequest(
      clusterName,
      CMSchedulePreferredLeaderElection(
        pleCancellable map { case (key, value) => (key, value._2) }
      )
    ))
  }

  def schedulePreferredLeaderElection(clusterName: String, topics: Set[String], timeIntervalMinutes: Int): Future[String] = {
    implicit val ec = apiExecutionContext

    pleCancellable += (clusterName ->
      (
        Some(
          system.scheduler.scheduleAtFixedRate(0 seconds, Duration(timeIntervalMinutes, TimeUnit.MINUTES)) {
            () => {
              runPreferredLeaderElectionWithAllTopics(clusterName)
            }
          }
        ),
        timeIntervalMinutes
      )
    )
    updateSchedulePreferredLeaderElection(clusterName)

    Future("Scheduler started")
  }

  def cancelPreferredLeaderElection(clusterName: String): Future[String] = {
    implicit val ec = apiExecutionContext

    pleCancellable(clusterName)._1.map(_.cancel())
    pleCancellable -= clusterName
    updateSchedulePreferredLeaderElection(clusterName)
    Future("Scheduler stopped")
  }

  def manualPartitionAssignments(clusterName: String,
                                 assignments: List[(String, List[(Int, List[Int])])]) = {
    implicit val ec = apiExecutionContext
    val results = tryWithKafkaManagerActor(
      KMClusterCommandRequest (
        clusterName,
        CMManualPartitionAssignments(assignments)
      )
    ) { result: CMCommandResults =>
      val errors = result.result.collect { case Failure(t) => ApiError(t.getMessage)}
      if (errors.isEmpty)
        \/-({})
      else
        -\/(errors)
    }

    results.map {
      case -\/(e) => -\/(IndexedSeq(e))
      case \/-(lst) => lst
    }
  }

  def generatePartitionAssignments(
                                    clusterName: String,
                                    topics: Set[String],
                                    brokers: Set[Int],
                                    replicationFactor: Option[Int] = None
                                    ): Future[IndexedSeq[ApiError] \/ Unit] =
  {
    val results = tryWithKafkaManagerActor(
      KMClusterCommandRequest(
        clusterName,
        CMGeneratePartitionAssignments(topics, brokers, replicationFactor)
      )
    ) { result: CMCommandResults =>
      val errors = result.result.collect { case Failure(t) => ApiError(t.getMessage)}
      if (errors.isEmpty)
        \/-({})
      else
        -\/(errors)
    }
    implicit val ec = apiExecutionContext
    results.map {
      case -\/(e) => -\/(IndexedSeq(e))
      case \/-(lst) => lst
    }
  }

  def runReassignPartitions(clusterName: String, topics: Set[String], force: Boolean = false): Future[IndexedSeq[ApiError] \/ Unit] = {
    implicit val ec = apiExecutionContext
    val forceSet: Set[ForceReassignmentCommand] = {
      if(force) {
        Set(ForceOnReplicationOutOfSync)
      } else Set.empty
    }
    val results = tryWithKafkaManagerActor(KMClusterCommandRequest(clusterName, CMRunReassignPartition(topics, forceSet))) {
      resultFuture: Future[CMCommandResults] =>
        resultFuture map { result =>
          val errors = result.result.collect {
            case Failure(t) =>
              t match {
                case UtilException(e) if e.isInstanceOf[ReplicationOutOfSync] =>
                  ApiError(t.getMessage, recoverByForceOperation = true)
                case _ =>
                  ApiError(t.getMessage)
              }
          }
          if (errors.isEmpty)
            \/-({})
          else
            -\/(errors)
        }
    }
    results.flatMap {
      case \/-(lst) => lst
      case -\/(e) => Future.successful(-\/(IndexedSeq(e)))
    }
  }

  def createTopic(
                   clusterName: String,
                   topic: String,
                   partitions: Int,
                   replication: Int,
                   config: Properties = new Properties
                   ): Future[ApiError \/ ClusterContext] =
  {
    implicit val ec = apiExecutionContext
    withKafkaManagerActor(KMClusterCommandRequest(clusterName, CMCreateTopic(topic, partitions, replication, config))) {
      result: Future[CMCommandResult] =>
        result.map(cmr => toDisjunction(cmr.result))
    }
  }

  def addTopicPartitions(
                          clusterName: String,
                          topic: String,
                          brokers: Seq[Int],
                          partitions: Int,
                          readVersion: Int
                          ): Future[ApiError \/ ClusterContext] =
  {
    implicit val ec = apiExecutionContext
    getTopicIdentity(clusterName, topic).flatMap { topicIdentityOrError =>
      topicIdentityOrError.fold(
      e => Future.successful(-\/(e)), { ti =>
        val partitionReplicaList: Map[Int, Seq[Int]] = ti.partitionsIdentity.mapValues(_.replicas)
        withKafkaManagerActor(
          KMClusterCommandRequest(
            clusterName,
            CMAddTopicPartitions(topic, brokers, partitions, partitionReplicaList, readVersion)
          )
        ) {
          result: Future[CMCommandResult] =>
            result.map(cmr => toDisjunction(cmr.result))
        }
      }
      )
    }
  }

  def addMultipleTopicsPartitions(
                              clusterName: String,
                              topics: Seq[String],
                              brokers: Set[Int],
                              partitions: Int,
                              readVersions: Map[String, Int]
                              ): Future[ApiError \/ ClusterContext] =
  {
    implicit val ec = apiExecutionContext
    getTopicListExtended(clusterName).flatMap { tleOrError =>
      tleOrError.fold(
      e => Future.successful(-\/(e)), { tle =>
        // add partitions to only topics with topic identity
        val topicsAndReplicas = topicListSortedByNumPartitions(tle).filter(t => topics.contains(t._1) && t._2.nonEmpty).map{ case (t,i) => (t, i.get.partitionsIdentity.mapValues(_.replicas)) }
        withKafkaManagerActor(
          KMClusterCommandRequest(
            clusterName,
            CMAddMultipleTopicsPartitions(topicsAndReplicas, brokers, partitions, readVersions)
          )
        ) {
          result: Future[CMCommandResult] =>
            result.map(cmr => toDisjunction(cmr.result))
        }
      }
      )
    }
  }

  def updateTopicConfig(
                         clusterName: String,
                         topic: String,
                         config: Properties,
                         readVersion: Int
                         ): Future[ApiError \/ ClusterContext] =
  {
    implicit val ec = apiExecutionContext
    withKafkaManagerActor(
      KMClusterCommandRequest(
        clusterName,
        CMUpdateTopicConfig(topic, config, readVersion)
      )
    ) {
      result: Future[CMCommandResult] =>
        result.map(cmr => toDisjunction(cmr.result))
    }
  }

  def deleteTopic(
                   clusterName: String,
                   topic: String
                   ): Future[ApiError \/ ClusterContext] =
  {
    implicit val ec = apiExecutionContext
    withKafkaManagerActor(KMClusterCommandRequest(clusterName, CMDeleteTopic(topic))) {
      result: Future[CMCommandResult] =>
        result.map(cmr => toDisjunction(cmr.result))
    }
  }

  def createLogkafka(
                   clusterName: String,
                   logkafka_id: String,
                   log_path: String,
                   config: Properties = new Properties
                   ): Future[ApiError \/ ClusterContext] =
  {
    implicit val ec = apiExecutionContext
    withKafkaManagerActor(KMClusterCommandRequest(clusterName, CMCreateLogkafka(logkafka_id, log_path, config))) {
      result: Future[CMCommandResult] =>
        result.map(cmr => toDisjunction(cmr.result))
    }
  }

  def updateLogkafkaConfig(
                         clusterName: String,
                         logkafka_id: String,
                         log_path: String,
                         config: Properties,
                         checkConfig: Boolean = true
                         ): Future[ApiError \/ ClusterContext] =
  {
    implicit val ec = apiExecutionContext
    withKafkaManagerActor(
      KMClusterCommandRequest(
        clusterName,
        CMUpdateLogkafkaConfig(logkafka_id, log_path, config, checkConfig)
      )
    ) {
      result: Future[CMCommandResult] =>
        result.map(cmr => toDisjunction(cmr.result))
    }
  }

  def deleteLogkafka(
                   clusterName: String,
                   logkafka_id: String,
                   log_path: String
                   ): Future[ApiError \/ ClusterContext] =
  {
    implicit val ec = apiExecutionContext
    withKafkaManagerActor(KMClusterCommandRequest(clusterName, CMDeleteLogkafka(logkafka_id, log_path))) {
      result: Future[CMCommandResult] =>
        result.map(cmr => toDisjunction(cmr.result))
    }
  }

  //--------------------Queries--------------------------
  def getClusterConfig(clusterName: String): Future[ApiError \/ ClusterConfig] = {
    tryWithKafkaManagerActor(KMGetClusterConfig(clusterName)) { result: KMClusterConfigResult =>
      result.result.get
    }
  }

  def getClusterContext(clusterName: String): Future[ApiError \/ ClusterContext] = {
    tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, CMGetClusterContext))(
      identity[ClusterContext]
    )
  }

  def getClusterList: Future[ApiError \/ KMClusterList] = {
    tryWithKafkaManagerActor(KMGetAllClusters)(identity[KMClusterList])
  }

  def getClusterView(clusterName: String): Future[ApiError \/ CMView] = {
    tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, CMGetView))(identity[CMView])
  }

  def getTopicList(clusterName: String): Future[ApiError \/ TopicList] = {
    tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, KSGetTopics))(identity[TopicList])
  }

  def getTopicListExtended(clusterName: String): Future[ApiError \/ TopicListExtended] = {
    val futureTopicIdentities = tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, BVGetTopicIdentities))(
      identity[Map[String, TopicIdentity]])
    val futureTopicList = tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, KSGetTopics))(identity[TopicList])
    val futureTopicToConsumerMap = tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, BVGetTopicConsumerMap))(
      identity[Map[String, Iterable[(String, ConsumerType)]]])
    val futureTopicsReasgn = getTopicsUnderReassignment(clusterName)
    implicit val ec = apiExecutionContext
    for {
      errOrTi <- futureTopicIdentities
      errOrTl <- futureTopicList
      errOrTCm <- futureTopicToConsumerMap
      errOrRap <- futureTopicsReasgn
    } yield {
      for {
        ti <- errOrTi
        tl <- errOrTl
        tcm <- errOrTCm
        rap <- errOrRap
      } yield {
        TopicListExtended(tl.list.map(t => (t, ti.get(t))).sortBy(_._1), tcm, tl.deleteSet, rap, tl.clusterContext)
      }
    }
  }

  def getConsumerListExtended(clusterName: String): Future[ApiError \/ ConsumerListExtended] = {
    val futureConsumerIdentities = tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, BVGetConsumerIdentities))(
      identity[Map[(String, ConsumerType), ConsumerIdentity]])
    val futureConsumerList = tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, KSGetConsumers))(identity[ConsumerList])
    implicit val ec = apiExecutionContext
    for {
      errorOrCI <- futureConsumerIdentities
      errorOrCL <- futureConsumerList
    } yield {
      for {
        ci <- errorOrCI
        cl <- errorOrCL
      } yield {
        ConsumerListExtended(cl.list.map(c => ((c.name, c.consumerType), ci.get((c.name, c.consumerType)))), cl.clusterContext)
      }
    }
  }

  def getTopicsUnderReassignment(clusterName: String): Future[ApiError \/ IndexedSeq[String]] = {
    val futureReassignments = getReassignPartitions(clusterName)
    implicit val ec = apiExecutionContext
    futureReassignments.map {
      case -\/(e) => -\/(e)
      case \/-(rap) =>
        \/-(rap.map { asgn =>
          asgn.endTime.map(_ => IndexedSeq()).getOrElse{
            asgn.partitionsToBeReassigned.map { case (t, s) => t.topic}.toSet.toIndexedSeq
          }
        }.getOrElse{IndexedSeq()})
    }
  }

  def getBrokerList(clusterName: String): Future[ApiError \/ BrokerListExtended] = {
    val futureBrokerList= tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, KSGetBrokers))(identity[BrokerList])
    val futureBrokerMetrics = tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, BVGetBrokerMetrics))(identity[Map[Int,BrokerMetrics]])
    implicit val ec = apiExecutionContext
    futureBrokerList.flatMap[ApiError \/ BrokerListExtended] { errOrBrokerList =>
      errOrBrokerList.fold ({
        err: ApiError => Future.successful(-\/(err))
      }, { bl =>
        for {
          errOrbm <- futureBrokerMetrics.recover[ApiError \/ Map[Int,BrokerMetrics]] { case t =>
            \/-(Map.empty)
          }
        } yield {
          val bm = errOrbm.toOption.getOrElse(Map.empty)
          \/-(
            BrokerListExtended(
              bl.list,
              bm,
              if(bm.isEmpty) None else Option(bm.values.foldLeft(BrokerMetrics.DEFAULT)((acc, m) => acc + m)),
              bl.clusterContext
            ))
        }
      })
    }
  }

  def getBrokersView(clusterName: String): Future[\/[ApiError, Map[Int, BVView]]] = {
    implicit val ec = apiExecutionContext

    tryWithKafkaManagerActor(
      KMClusterQueryRequest(
        clusterName,
        BVGetViews
      )
    )(identity[Map[Int, BVView]])
  }

  def getBrokerView(clusterName: String, brokerId: Int): Future[ApiError \/ BVView] = {
    val futureView = tryWithKafkaManagerActor(
      KMClusterQueryRequest(
        clusterName,
        BVGetView(brokerId)
      )
    )(identity[Option[BVView]])
    implicit val ec = apiExecutionContext
    futureView.flatMap[ApiError \/ BVView] { errOrView =>
      errOrView.fold(
      { err: ApiError =>
        Future.successful(-\/[ApiError](err))
      }, { viewOption: Option[BVView] =>
        viewOption.fold {
          Future.successful[ApiError \/ BVView](-\/(ApiError(s"Broker not found $brokerId for cluster $clusterName")))
        } { view =>
          Future.successful(\/-(view))
        }
      }
      )
    }
  }

  def getTopicIdentity(clusterName: String, topic: String): Future[ApiError \/ TopicIdentity] = {
    val futureCMTopicIdentity = tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, CMGetTopicIdentity(topic)))(
      identity[Option[CMTopicIdentity]]
    )
    implicit val ec = apiExecutionContext
    futureCMTopicIdentity.map[ApiError \/ TopicIdentity] { errOrTI =>
      errOrTI.fold[ApiError \/ TopicIdentity](
      { err: ApiError =>
        -\/[ApiError](err)
      }, { tiOption: Option[CMTopicIdentity] =>
        tiOption.fold[ApiError \/ TopicIdentity] {
          -\/(ApiError(s"Topic not found $topic for cluster $clusterName"))
        } { cmTopicIdentity =>
          cmTopicIdentity.topicIdentity match {
            case scala.util.Failure(t) =>
              -\/[ApiError](t)
            case scala.util.Success(ti) =>
              \/-(ti)
          }
        }
      }
      )
    }
  }

  def getConsumersForTopic(clusterName: String, topic: String): Future[Option[Iterable[(String, ConsumerType)]]] = {
    val futureTopicConsumerMap = tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, BVGetTopicConsumerMap))(
      identity[Map[String, Iterable[(String, ConsumerType)]]])
    implicit val ec = apiExecutionContext
    futureTopicConsumerMap.map[Option[Iterable[(String, ConsumerType)]]] { errOrTCM =>
      errOrTCM.fold[Option[Iterable[(String, ConsumerType)]]] (_ => None, _.get(topic))
    }
  }

  def getGeneratedAssignments(clusterName: String, topic: String): Future[ApiError \/ GeneratedPartitionAssignments] = {
    tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, CMGetGeneratedPartitionAssignments(topic)))(
      identity[GeneratedPartitionAssignments]
    )
  }

  private[this] def tryWithConsumerType[T](consumerType: String)(fn: ConsumerType => Future[ApiError \/ T])(implicit ec: ExecutionContext) : Future[ApiError \/ T] = {
    Future.successful[ApiError \/ ConsumerType] {
      ConsumerType.from(consumerType).fold[ApiError \/ ConsumerType](-\/(ApiError(s"Unknown consumer type : $consumerType"))){
        ct => \/-(ct)
      }
    }.flatMap { ctOrError =>
      ctOrError.fold(err => Future.successful[ApiError \/ T](-\/(err)), fn)
    }
  }

  def getConsumerIdentity(clusterName: String, consumer: String, consumerType: String): Future[ApiError \/ ConsumerIdentity] = {
    implicit val ec = apiExecutionContext
    val futureCMConsumerIdentity = tryWithConsumerType(consumerType) { ct =>
        tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, CMGetConsumerIdentity(consumer, ct)))(
          identity[CMConsumerIdentity]
        )
    }
    futureCMConsumerIdentity.map[ApiError \/ ConsumerIdentity] { errOrCI =>
      errOrCI.fold[ApiError \/ ConsumerIdentity](
      { err: ApiError =>
        -\/[ApiError](err)
      }, { ci: CMConsumerIdentity =>
          ci.consumerIdentity match {
            case scala.util.Failure(c) =>
              -\/[ApiError](c)
            case scala.util.Success(ci) =>
              \/-(ci)
          }
      })
    }
  }

  def getConsumedTopicState(clusterName: String, consumer: String, topic: String, consumerType: String): Future[ApiError \/ ConsumedTopicState] = {
    implicit val ec = apiExecutionContext
    val futureCMConsumedTopic = tryWithConsumerType(consumerType) { ct =>
      tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, CMGetConsumedTopicState(consumer, topic, ct)))(
        identity[CMConsumedTopic]
      )
    }
    futureCMConsumedTopic.map[ApiError \/ ConsumedTopicState] { errOrCT =>
      errOrCT.fold[ApiError \/ ConsumedTopicState](
      { err: ApiError =>
        -\/[ApiError](err)
      }, { cmConsumedTopic: CMConsumedTopic =>
        cmConsumedTopic.ctIdentity match {
          case scala.util.Failure(c) =>
            -\/[ApiError](c)
          case scala.util.Success(ci) =>
            \/-(ci)
        }
      }
      )
    }
  }

  def getTopicMetrics(clusterName: String, topic: String): Future[ApiError \/ Option[BrokerMetrics]] = {
    tryWithKafkaManagerActor(
      KMClusterQueryRequest(
        clusterName,
        BVGetTopicMetrics(topic)
      )
    ) { brokerMetrics: Option[BrokerMetrics] =>
      brokerMetrics
    }
  }

  def getPreferredLeaderElection(clusterName: String): Future[ApiError \/ Option[PreferredReplicaElection]] = {
    tryWithKafkaManagerActor(
      KMClusterQueryRequest(
        clusterName,
        KSGetPreferredLeaderElection
      )
    )(identity[Option[PreferredReplicaElection]])
  }

  def getReassignPartitions(clusterName: String): Future[ApiError \/ Option[ReassignPartitions]] = {
    tryWithKafkaManagerActor(
      KMClusterQueryRequest(
        clusterName,
        KSGetReassignPartition
      )
    )(identity[Option[ReassignPartitions]])
  }

  def topicListSortedByNumPartitions(tle: TopicListExtended): Seq[(String, Option[TopicIdentity])] = {
    def partition(tiOption: Option[TopicIdentity]): Int = {
      tiOption match {
        case Some(ti) => ti.partitions
        case None => 0
      }
    }
    val sortedByNumPartition = tle.list.sortWith{ (leftE, rightE) =>
      partition(leftE._2) > partition(rightE._2)
    }
    sortedByNumPartition
  }

  def getLogkafkaLogkafkaIdList(clusterName: String): Future[ApiError \/ LogkafkaLogkafkaIdList] = {
    tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, LKSGetLogkafkaLogkafkaIds))(identity[LogkafkaLogkafkaIdList])
  }

  def getLogkafkaListExtended(clusterName: String): Future[ApiError \/ LogkafkaListExtended] = {
    val futureLogkafkaIdentities = tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, LKVGetLogkafkaIdentities))(identity[Map[String, LogkafkaIdentity]])
    val futureLogkafkaList = tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, LKSGetLogkafkaLogkafkaIds))(identity[LogkafkaLogkafkaIdList])
    implicit val ec = apiExecutionContext
    for {
      errOrLi <- futureLogkafkaIdentities
      errOrLl <- futureLogkafkaList
    } yield {
      for {
        li <- errOrLi
        ll <- errOrLl
      } yield {
        LogkafkaListExtended(ll.list.map(l => (l, li.get(l))), ll.deleteSet)
      }
    }
  }

  def getLogkafkaIdentity(clusterName: String, logkafka_id: String): Future[ApiError \/ LogkafkaIdentity] = {
    val futureCMLogkafkaIdentity = tryWithKafkaManagerActor(KMClusterQueryRequest(clusterName, CMGetLogkafkaIdentity(logkafka_id)))(
      identity[Option[CMLogkafkaIdentity]]
    )
    implicit val ec = apiExecutionContext
    futureCMLogkafkaIdentity.map[ApiError \/ LogkafkaIdentity] { errOrLI =>
      errOrLI.fold[ApiError \/ LogkafkaIdentity](
      { err: ApiError =>
        -\/[ApiError](err)
      }, { liOption: Option[CMLogkafkaIdentity] =>
        liOption.fold[ApiError \/ LogkafkaIdentity] {
          -\/(ApiError(s"Logkafka not found $logkafka_id for cluster $clusterName"))
        } { cmLogkafkaIdentity =>
          cmLogkafkaIdentity.logkafkaIdentity match {
            case scala.util.Failure(l) =>
              -\/[ApiError](l)
            case scala.util.Success(li) =>
              \/-(li)
          }
        }
      }
      )
    }
  }

  def initialiseSchedulePreferredLeaderElection(): Unit = {
    implicit val ec = apiExecutionContext
    implicit val formats = org.json4s.DefaultFormats

    var temp: Map[String, Int] = Map.empty
    val x = system.actorSelection(kafkaManagerActor).ask(KSGetScheduleLeaderElection)
    x.foreach { schedule =>
      temp = parse(schedule.toString).extract[Map[String, Int]]
      for ((cluster, timeInterval) <- temp) {
        schedulePreferredLeaderElection(cluster, Set(), timeInterval)
      }
    }
  }

  /* Contains a key for each cluster (by its name) which has preferred leader election scheduled
  * Value of each key is a 2-tuple where
  * * first element is the scheduler's cancellable object - required for cancelling the schedule
  * * second element is the time interval for scheduling (in minutes) - required for storing in ZK
  */
  var pleCancellable : Map[String, (Option[Cancellable], Int)] = Map.empty
  initialiseSchedulePreferredLeaderElection()

}
