/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.actor

import akka.actor.Cancellable
import kafka.manager._
import kafka.manager.base.BaseCommandActor
import kafka.manager.model.ActorModel.{ActorResponse, CommandRequest, DCUpdateState}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}

import scala.concurrent.duration._
import scala.util.Try

/**
 * @author hiral
 */
case class DeleteClusterActorConfig(curator: CuratorFramework,
                                    baseDeleteClustersZKPath: String,
                                    baseClustersZKPath: String,
                                    baseConfigsZKPath: String,
                                    updatePeriod: FiniteDuration = 10 seconds,
                                    deletionBatchSize: Int = 2)
class DeleteClusterActor(config: DeleteClusterActorConfig) extends BaseCommandActor {
  private[this] var cancellable : Option[Cancellable] = None

  private[this] val deleteClustersPathCache = new PathChildrenCache(config.curator,config.baseDeleteClustersZKPath,true)

  private[this] val pathCacheListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      log.debug(s"Got event : ${event.getType}")
      event.getType match {
        case PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED | PathChildrenCacheEvent.Type.CHILD_ADDED |
             PathChildrenCacheEvent.Type.CHILD_UPDATED =>
          self ! DCUpdateState
        case _ => //don't care
      }
    }
  }

  @scala.throws[Exception](classOf[Exception])
  override def preStart() = {
    super.preStart()

    log.info("Started actor %s".format(self.path))

    log.info("Starting delete clusters path cache...")
    deleteClustersPathCache.start(StartMode.BUILD_INITIAL_CACHE)

    log.info("Adding kafka manager path cache listener...")
    deleteClustersPathCache.getListenable.addListener(pathCacheListener)

    log.info("Scheduling updater for %s".format(config.updatePeriod))
    cancellable = Some(
      context.system.scheduler.scheduleAtFixedRate(0 seconds,
        config.updatePeriod,
        self,
        DCUpdateState)(context.system.dispatcher,self)
    )

  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info("Stopped actor %s".format(self.path))

    log.info("Removing delete clusters path cache listener...")
    Try(deleteClustersPathCache.getListenable.removeListener(pathCacheListener))

    log.info("Shutting down delete clusters path cache...")
    Try(deleteClustersPathCache.close())

    super.postStop()
  }

  override def processCommandRequest(request: CommandRequest): Unit = {
    request match  {
      case DCUpdateState =>
        import scala.collection.JavaConverters._
        Option(deleteClustersPathCache.getCurrentData).foreach { list =>
          list.asScala.take(config.deletionBatchSize).foreach { cd =>
            val cn = nodeFromPath(cd.getPath)
            val zkpath = s"${config.baseClustersZKPath}/$cn"
            log.info(s"Attempting to clean deleted cluster path : $zkpath")
            Try { cleanClusterPath(zkpath) }.flatMap { _ =>
              Try {
                if(config.curator.checkExists().forPath(zkpath) != null) {
                  log.info(s"Deleting node $zkpath")
                  config.curator.delete().deletingChildrenIfNeeded().forPath(zkpath)
                }
              }
            }.flatMap { _ =>
              Try {
                val configPath = s"${config.baseConfigsZKPath}/$cn"
                if(config.curator.checkExists().forPath(configPath) != null) {
                  log.info(s"Deleting node $configPath")
                  config.curator.delete().forPath(configPath)
                }
              }
            }.flatMap { _ =>
              Try {
                log.info(s"Deleting node ${cd.getPath}")
                config.curator.delete().forPath(cd.getPath)
              }
            }
          }
        }
      case any: Any => log.warning("dca : processCommandRequest : Received unknown message: {}", any)
    }
  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case any: Any => log.warning("dca : processActorResponse : Received unknown message: {}", any)
    }
  }

  private[this] def cleanClusterPath(zkpath: String) : Unit = {
    import scala.collection.JavaConverters._
    Option(config.curator.checkExists().forPath(zkpath)).foreach { stat =>
      //path exists, attempt to get children
      Option(config.curator.getChildren.forPath(zkpath)).foreach { children =>
        //children exist, delete them
        children.asScala.foreach { child =>
          val childPath = s"$zkpath/$child"
          log.info(s"Deleting node $childPath")
          config.curator.delete().deletingChildrenIfNeeded().forPath(childPath)
        }
      }
    }
  }
}
