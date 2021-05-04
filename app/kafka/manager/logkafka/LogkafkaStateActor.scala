/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.logkafka

import kafka.manager._
import kafka.manager.base.BaseQueryCommandActor
import kafka.manager.features.KMLogKafkaFeature
import kafka.manager.model.ActorModel._
import kafka.manager.model.ClusterContext
import kafka.manager.utils.LogkafkaZkUtils
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache._
import org.joda.time.{DateTime, DateTimeZone}

import scala.util.{Failure, Success, Try}

/**
 * @author hiral
 */
import scala.collection.JavaConverters._
class LogkafkaStateActor(curator: CuratorFramework, 
                      clusterContext: ClusterContext) extends BaseQueryCommandActor {

  private[this] val logkafkaConfigTreeCache = new TreeCache(curator,LogkafkaZkUtils.LogkafkaConfigPath)

  private[this] val logkafkaClientTreeCache = new TreeCache(curator,LogkafkaZkUtils.LogkafkaClientPath)

  @volatile
  private[this] var logkafkaConfigTreeCacheLastUpdateMillis : Long = System.currentTimeMillis()

  @volatile
  private[this] var logkafkaClientTreeCacheLastUpdateMillis : Long = System.currentTimeMillis()

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

  @scala.throws[Exception](classOf[Exception])
  override def preStart() = {
    if (clusterContext.clusterFeatures.features(KMLogKafkaFeature)) {
      log.info("Started actor %s".format(self.path))
      log.info("Starting logkafka config tree cache...")
      logkafkaConfigTreeCache.start()
      log.info("Starting logkafka client tree cache...")
      logkafkaClientTreeCache.start()

      log.info("Adding logkafka config tree cache listener...")
      logkafkaConfigTreeCache.getListenable.addListener(logkafkaConfigTreeCacheListener)
      log.info("Adding logkafka client tree cache listener...")
      logkafkaClientTreeCache.getListenable.addListener(logkafkaClientTreeCacheListener)
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

    log.info("Removing logkafka config tree cache listener...")
    Try(logkafkaConfigTreeCache.getListenable.removeListener(logkafkaConfigTreeCacheListener))
    log.info("Removing logkafka client tree cache listener...")
    Try(logkafkaClientTreeCache.getListenable.removeListener(logkafkaClientTreeCacheListener))

    log.info("Shutting down logkafka config tree cache...")
    Try(logkafkaConfigTreeCache.close())
    log.info("Shutting down logkafka client tree cache...")
    Try(logkafkaClientTreeCache.close())

    super.postStop()
  }

  def getLogkafkaConfig(logkafka_id: String) : Option[LogkafkaConfig] = {
      for {
        config <- getLogkafkaConfigString(logkafka_id)
      } yield LogkafkaConfig(logkafka_id, Some(config))
  }

  def getLogkafkaClient(logkafka_id: String) : Option[LogkafkaClient] = {
      for {
        client <- getLogkafkaClientString(logkafka_id)
      } yield LogkafkaClient(logkafka_id, Some(client))
  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case any: Any => log.warning("lksa : processActorResponse : Received unknown message: {}", any.toString)
    }
  }
  
  private[this] def getLogkafkaConfigString(logkafka_id: String) : Option[String] = {
    val logkafka_idPath = "%s/%s".format(LogkafkaZkUtils.LogkafkaConfigPath,logkafka_id)
    Option(logkafkaConfigTreeCache.getCurrentData(logkafka_idPath)).map( childData => asString(childData.getData))
  }

  private[this] def getLogkafkaClientString(logkafka_id: String) : Option[String] = {
    val logkafka_idPath = "%s/%s".format(LogkafkaZkUtils.LogkafkaClientPath,logkafka_id)
    Option(logkafkaClientTreeCache.getCurrentData(logkafka_idPath)).map( childData => asString(childData.getData))
  }

  override def processQueryRequest(request: QueryRequest): Unit = {
    request match {
      case LKSGetLogkafkaLogkafkaIds =>
        val deleteSet: Set[String] = Set.empty
        withLogkafkaConfigTreeCache { cache =>
          cache.getCurrentChildren(LogkafkaZkUtils.LogkafkaConfigPath)
        }.fold {
          sender ! LogkafkaLogkafkaIdList(IndexedSeq.empty, deleteSet)
        } { data: java.util.Map[String, ChildData] =>
          sender ! LogkafkaLogkafkaIdList(data.asScala.map(kv => kv._1).toIndexedSeq, deleteSet)
        }

      case LKSGetLogkafkaConfig(logkafka_id) =>
        sender ! getLogkafkaConfig(logkafka_id)

      case LKSGetLogkafkaClient(logkafka_id) =>
        sender ! getLogkafkaClient(logkafka_id)

      case LKSGetLogkafkaConfigs(logkafka_ids) =>
        sender ! LogkafkaConfigs(logkafka_ids.toIndexedSeq.map(getLogkafkaConfig).flatten, logkafkaConfigTreeCacheLastUpdateMillis)

      case LKSGetLogkafkaClients(logkafka_ids) =>
        sender ! LogkafkaClients(logkafka_ids.toIndexedSeq.map(getLogkafkaClient).flatten, logkafkaClientTreeCacheLastUpdateMillis)

      case LKSGetAllLogkafkaConfigs(lastUpdateMillisOption) =>
        val lastUpdateMillis = lastUpdateMillisOption.getOrElse(0L)
        if (logkafkaConfigTreeCacheLastUpdateMillis > lastUpdateMillis) {
          //we have option here since there may be no logkafka configs at all!
          withLogkafkaConfigTreeCache {  cache: TreeCache =>
            cache.getCurrentChildren(LogkafkaZkUtils.LogkafkaConfigPath)
          }.fold {
            sender ! LogkafkaConfigs(IndexedSeq.empty, logkafkaConfigTreeCacheLastUpdateMillis)
          } { data: java.util.Map[String, ChildData] =>
            sender ! LogkafkaConfigs(data.asScala.keys.toIndexedSeq.map(getLogkafkaConfig).flatten, logkafkaConfigTreeCacheLastUpdateMillis)
          }
        } // else no updates to send

      case LKSGetAllLogkafkaClients(lastUpdateMillisOption) =>
        val lastUpdateMillis = lastUpdateMillisOption.getOrElse(0L)
        if (logkafkaClientTreeCacheLastUpdateMillis > lastUpdateMillis) {
          //we have option here since there may be no logkafka clients at all!
          withLogkafkaClientTreeCache {  cache: TreeCache =>
            cache.getCurrentChildren(LogkafkaZkUtils.LogkafkaClientPath)
          }.fold {
            sender ! LogkafkaClients(IndexedSeq.empty, logkafkaClientTreeCacheLastUpdateMillis)
          } { data: java.util.Map[String, ChildData] =>
            sender ! LogkafkaClients(data.asScala.keys.toIndexedSeq.map(getLogkafkaClient).flatten, logkafkaClientTreeCacheLastUpdateMillis)
          }
        } // else no updates to send

      case any: Any => log.warning("lksa : processQueryRequest : Received unknown message: {}", any.toString)
    }
  }

  override def processCommandRequest(request: CommandRequest): Unit = {
    request match {
      case any: Any => log.warning("lksa : processCommandRequest : Received unknown message: {}", any.toString)
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

  private[this] def withLogkafkaConfigTreeCache[T](fn: TreeCache => T) : Option[T] = {
    Option(fn(logkafkaConfigTreeCache))
  }

  private[this] def withLogkafkaClientTreeCache[T](fn: TreeCache => T) : Option[T] = {
    Option(fn(logkafkaClientTreeCache))
  }

}

