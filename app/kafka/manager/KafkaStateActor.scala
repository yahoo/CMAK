/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import kafka.manager.utils.zero81.{ReassignPartitionCommand, PreferredReplicaLeaderElectionCommand}
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.cache._
import org.apache.curator.framework.CuratorFramework
import org.joda.time.{DateTimeZone, DateTime}
import kafka.manager.utils.{TopicAndPartition, ZkUtils}

import scala.collection.mutable
import scala.util.{Success, Failure, Try}

/**
 * @author hiral
 */
import ActorModel._
import kafka.manager.utils._
import scala.collection.JavaConverters._
class KafkaStateActor(curator: CuratorFramework, 
                      deleteSupported: Boolean, 
                      clusterConfig: ClusterConfig) extends BaseQueryCommandActor {

  // e.g. /brokers/topics/analytics_content/partitions/0/state
  private[this] val topicsTreeCache = new TreeCache(curator,ZkUtils.BrokerTopicsPath)

  private[this] val topicsConfigPathCache = new PathChildrenCache(curator,ZkUtils.TopicConfigPath,true)

  private[this] val brokersPathCache = new PathChildrenCache(curator,ZkUtils.BrokerIdsPath,true)

  private[this] val adminPathCache = new PathChildrenCache(curator,ZkUtils.AdminPath,true)
  
  private[this] val deleteTopicsPathCache = new PathChildrenCache(curator, ZkUtils.DeleteTopicsPath,true)

  @volatile
  private[this] var topicsTreeCacheLastUpdateMillis : Long = System.currentTimeMillis()

  private[this] val logkafkaConfigTreeCache = new TreeCache(curator,ZkUtils.LogkafkaConfigPath)

  private[this] val logkafkaClientTreeCache = new TreeCache(curator,ZkUtils.LogkafkaClientPath)

  @volatile
  private[this] var logkafkaConfigTreeCacheLastUpdateMillis : Long = System.currentTimeMillis()

  @volatile
  private[this] var logkafkaClientTreeCacheLastUpdateMillis : Long = System.currentTimeMillis()

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

  private[this] val logkafkaConfigTreeCacheListener = new TreeCacheListener {
    override def childEvent(client: CuratorFramework, event: TreeCacheEvent): Unit = {
      event.getType match {
        case TreeCacheEvent.Type.INITIALIZED | TreeCacheEvent.Type.NODE_ADDED |
             TreeCacheEvent.Type.NODE_REMOVED | TreeCacheEvent.Type.NODE_UPDATED =>
          logkafkaConfigTreeCacheLastUpdateMillis = System.currentTimeMillis()
        case _ =>
          //do nothing
      }
    }
  }

  private[this] val logkafkaClientTreeCacheListener = new TreeCacheListener {
    override def childEvent(client: CuratorFramework, event: TreeCacheEvent): Unit = {
      event.getType match {
        case TreeCacheEvent.Type.INITIALIZED | TreeCacheEvent.Type.NODE_ADDED |
             TreeCacheEvent.Type.NODE_REMOVED | TreeCacheEvent.Type.NODE_UPDATED =>
          logkafkaClientTreeCacheLastUpdateMillis = System.currentTimeMillis()
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

  @scala.throws[Exception](classOf[Exception])
  override def preStart() = {
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
    log.info("Starting logkafka config tree cache...")
    logkafkaConfigTreeCache.start()
    log.info("Starting logkafka client tree cache...")
    logkafkaClientTreeCache.start()

    log.info("Adding topics tree cache listener...")
    topicsTreeCache.getListenable.addListener(topicsTreeCacheListener)
    log.info("Adding logkafka config tree cache listener...")
    logkafkaConfigTreeCache.getListenable.addListener(logkafkaConfigTreeCacheListener)
    log.info("Adding logkafka client tree cache listener...")
    logkafkaClientTreeCache.getListenable.addListener(logkafkaClientTreeCacheListener)
    log.info("Adding admin path cache listener...")
    adminPathCache.getListenable.addListener(adminPathCacheListener)
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

    log.info("Removing admin path cache listener...")
    Try(adminPathCache.getListenable.removeListener(adminPathCacheListener))
    log.info("Removing topics tree cache listener...")
    Try(topicsTreeCache.getListenable.removeListener(topicsTreeCacheListener))
    log.info("Removing logkafka config tree cache listener...")
    Try(logkafkaConfigTreeCache.getListenable.removeListener(logkafkaConfigTreeCacheListener))
    log.info("Removing logkafka client tree cache listener...")
    Try(logkafkaClientTreeCache.getListenable.removeListener(logkafkaClientTreeCacheListener))

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
    log.info("Shutting down logkafka config tree cache...")
    Try(logkafkaConfigTreeCache.close())
    log.info("Shutting down logkafka client tree cache...")
    Try(logkafkaClientTreeCache.close())

    super.postStop()
  }

  def getTopicDescription(topic: String) : Option[TopicDescription] = {
    val topicPath = "%s/%s".format(ZkUtils.BrokerTopicsPath,topic)
    val descriptionOption : Option[(Int,String)] =
      Option(topicsTreeCache.getCurrentData(topicPath)).map( childData => (childData.getStat.getVersion,asString(childData.getData)))

    for {
      description <- descriptionOption
      partitionsPath = "%s/%s/partitions".format(ZkUtils.BrokerTopicsPath, topic)
      partitions: Map[String, ChildData] <- Option(topicsTreeCache.getCurrentChildren(partitionsPath)).map(_.asScala.toMap)
      states : Map[String, String] = partitions flatMap { case (part, _) =>
        val statePath = s"$partitionsPath/$part/state"
        Option(topicsTreeCache.getCurrentData(statePath)).map(cd => (part, asString(cd.getData)))
      }
      config = getTopicConfigString(topic)
    } yield TopicDescription(topic, description, Option(states),config, deleteSupported)
  }

  def getLogkafkaConfig(hostname: String) : Option[LogkafkaConfig] = {
      for {
        config <- getLogkafkaConfigString(hostname)
      } yield LogkafkaConfig(hostname, Some(config))
  }

  def getLogkafkaClient(hostname: String) : Option[LogkafkaClient] = {
      for {
        client <- getLogkafkaClientString(hostname)
      } yield LogkafkaClient(hostname, Some(client))
  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case any: Any => log.warning("ksa : processActorResponse : Received unknown message: {}", any.toString)
    }
  }
  
  private[this] def getTopicConfigString(topic: String) : Option[(Int,String)] = {
    val data: mutable.Buffer[ChildData] = topicsConfigPathCache.getCurrentData.asScala
    val result: Option[ChildData] = data.find(p => p.getPath.endsWith(topic))
    result.map(cd => (cd.getStat.getVersion,asString(cd.getData)))
  }

  private[this] def getLogkafkaConfigString(hostname: String) : Option[String] = {
    val hostnamePath = "%s/%s".format(ZkUtils.LogkafkaConfigPath,hostname)
    Option(logkafkaConfigTreeCache.getCurrentData(hostnamePath)).map( childData => asString(childData.getData))
  }

  private[this] def getLogkafkaClientString(hostname: String) : Option[String] = {
    val hostnamePath = "%s/%s".format(ZkUtils.LogkafkaClientPath,hostname)
    Option(logkafkaClientTreeCache.getCurrentData(hostnamePath)).map( childData => asString(childData.getData))
  }

  override def processQueryRequest(request: QueryRequest): Unit = {
    request match {
      case KSGetTopics =>
        val deleteSet: Set[String] = {
          if(deleteSupported) {
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
          sender ! TopicList(IndexedSeq.empty, deleteSet)
        } { data: java.util.Map[String, ChildData] =>
          sender ! TopicList(data.asScala.map(kv => kv._1).toIndexedSeq, deleteSet)
        }

      case KSGetTopicConfig(topic) =>
        sender ! TopicConfig(topic, getTopicConfigString(topic))

      case KSGetTopicDescription(topic) =>
        sender ! getTopicDescription(topic)

      case KSGetTopicDescriptions(topics) =>
        sender ! TopicDescriptions(topics.toIndexedSeq.map(getTopicDescription).flatten, topicsTreeCacheLastUpdateMillis)

      case KSGetAllTopicDescriptions(lastUpdateMillisOption) =>
        val lastUpdateMillis = lastUpdateMillisOption.getOrElse(0L)
        if (topicsTreeCacheLastUpdateMillis > lastUpdateMillis) {
          //we have option here since there may be no topics at all!
          withTopicsTreeCache {  cache: TreeCache =>
            cache.getCurrentChildren(ZkUtils.BrokerTopicsPath)
          }.fold {
            sender ! TopicDescriptions(IndexedSeq.empty, topicsTreeCacheLastUpdateMillis)
          } { data: java.util.Map[String, ChildData] =>
            sender ! TopicDescriptions(data.asScala.keys.toIndexedSeq.map(getTopicDescription).flatten, topicsTreeCacheLastUpdateMillis)
          }
        } // else no updates to send

      case KSGetTopicsLastUpdateMillis =>
        sender ! topicsTreeCacheLastUpdateMillis

      case KSGetBrokers =>
        val data: mutable.Buffer[ChildData] = brokersPathCache.getCurrentData.asScala
        val result: IndexedSeq[BrokerIdentity] = data.map { cd =>
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
        sender ! BrokerList(result, clusterConfig)

      case KSGetPreferredLeaderElection =>
        sender ! preferredLeaderElection

      case KSGetReassignPartition =>
        sender ! reassignPartitions

      case KSGetLogkafkaHostnames =>
        val deleteSet: Set[String] = Set.empty
        withLogkafkaConfigTreeCache { cache =>
          cache.getCurrentChildren(ZkUtils.LogkafkaConfigPath)
        }.fold {
          sender ! LogkafkaHostnameList(IndexedSeq.empty, deleteSet)
        } { data: java.util.Map[String, ChildData] =>
          sender ! LogkafkaHostnameList(data.asScala.map(kv => kv._1).toIndexedSeq, deleteSet)
        }

      case KSGetLogkafkaConfig(hostname) =>
        sender ! getLogkafkaConfig(hostname)

      case KSGetLogkafkaClient(hostname) =>
        sender ! getLogkafkaClient(hostname)

      case KSGetLogkafkaConfigs(hostnames) =>
        sender ! LogkafkaConfigs(hostnames.toIndexedSeq.map(getLogkafkaConfig).flatten, logkafkaConfigTreeCacheLastUpdateMillis)

      case KSGetLogkafkaClients(hostnames) =>
        sender ! LogkafkaClients(hostnames.toIndexedSeq.map(getLogkafkaClient).flatten, logkafkaClientTreeCacheLastUpdateMillis)

      case KSGetAllLogkafkaConfigs(lastUpdateMillisOption) =>
        val lastUpdateMillis = lastUpdateMillisOption.getOrElse(0L)
        if (logkafkaConfigTreeCacheLastUpdateMillis > lastUpdateMillis) {
          //we have option here since there may be no logkafka configs at all!
          withLogkafkaConfigTreeCache {  cache: TreeCache =>
            cache.getCurrentChildren(ZkUtils.LogkafkaConfigPath)
          }.fold {
            sender ! LogkafkaConfigs(IndexedSeq.empty, logkafkaConfigTreeCacheLastUpdateMillis)
          } { data: java.util.Map[String, ChildData] =>
            sender ! LogkafkaConfigs(data.asScala.keys.toIndexedSeq.map(getLogkafkaConfig).flatten, logkafkaConfigTreeCacheLastUpdateMillis)
          }
        } // else no updates to send

      case KSGetAllLogkafkaClients(lastUpdateMillisOption) =>
        val lastUpdateMillis = lastUpdateMillisOption.getOrElse(0L)
        if (logkafkaClientTreeCacheLastUpdateMillis > lastUpdateMillis) {
          //we have option here since there may be no logkafka clients at all!
          withLogkafkaClientTreeCache {  cache: TreeCache =>
            cache.getCurrentChildren(ZkUtils.LogkafkaClientPath)
          }.fold {
            sender ! LogkafkaClients(IndexedSeq.empty, logkafkaClientTreeCacheLastUpdateMillis)
          } { data: java.util.Map[String, ChildData] =>
            sender ! LogkafkaClients(data.asScala.keys.toIndexedSeq.map(getLogkafkaClient).flatten, logkafkaClientTreeCacheLastUpdateMillis)
          }
        } // else no updates to send

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
            preferredLeaderElection = Some(PreferredReplicaElection(getDateTime(millis), s, None))
          } {
            existing =>
              existing.endTime.fold {
                //update without end? Odd, copy existing
                preferredLeaderElection = Some(existing.copy(topicAndPartition = existing.topicAndPartition ++ s))
              } { _ =>
                //new op started
                preferredLeaderElection = Some(PreferredReplicaElection(getDateTime(millis), s, None))
              }
          }
        }
      case KSUpdateReassignPartition(millis,json) =>
        safeExecute {
          val m : Map[TopicAndPartition, Seq[Int]] = ReassignPartitionCommand.parsePartitionReassignmentZkData(json)
          reassignPartitions.fold {
            //nothing there, add as new
            reassignPartitions = Some(ReassignPartitions(getDateTime(millis),m, None))
          } {
            existing =>
              existing.endTime.fold {
                //update without end? Odd, copy existing
                reassignPartitions = Some(existing.copy(partitionsToBeReassigned = existing.partitionsToBeReassigned ++ m))
              } { _ =>
                //new op started
                reassignPartitions = Some(ReassignPartitions(getDateTime(millis),m, None))
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

  private[this] def withLogkafkaConfigTreeCache[T](fn: TreeCache => T) : Option[T] = {
    Option(fn(logkafkaConfigTreeCache))
  }

  private[this] def withLogkafkaClientTreeCache[T](fn: TreeCache => T) : Option[T] = {
    Option(fn(logkafkaClientTreeCache))
  }

}

