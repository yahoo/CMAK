/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.base

import akka.actor.ActorLogging
import kafka.manager.model.CuratorConfig
import org.apache.curator.RetrySleeper
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.BoundedExponentialBackoffRetry

import scala.util.Try

class LoggingRetryPolicy(curatorConfig: CuratorConfig, actorLogging: ActorLogging
                        ) extends BoundedExponentialBackoffRetry(curatorConfig.baseSleepTimeMs
  , curatorConfig.maxSleepTimeMs, curatorConfig.zkMaxRetry) {
  override def allowRetry(retryCount: Int, elapsedTimeMs: Long, sleeper: RetrySleeper): Boolean = {
    actorLogging.log.info(s"retryCount=$retryCount maxRetries=${curatorConfig.zkMaxRetry} zkConnect=${curatorConfig.zkConnect}")
    super.allowRetry(retryCount, elapsedTimeMs, sleeper)
  }
}

trait CuratorAwareActor extends BaseActor {
  
  protected def curatorConfig: CuratorConfig

  protected[this] val curator : CuratorFramework = getCurator(curatorConfig)
  log.info("Starting curator...")
  curator.start()

  protected def getCurator(config: CuratorConfig) : CuratorFramework = {
    val curator: CuratorFramework = CuratorFrameworkFactory.newClient(
      config.zkConnect,
      new LoggingRetryPolicy(config, this))
    curator
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info("Shutting down curator...")
    Try(curator.close())
    super.postStop()
  }
}

trait BaseZkPath {
  this : CuratorAwareActor =>

  protected def baseZkPath : String

  protected def zkPath(path: String): String = {
    require(path.nonEmpty, "path must be nonempty")
    "%s/%s" format(baseZkPath, path)
  }

  protected def zkPathFrom(parent: String,child: String): String = {
    require(parent.nonEmpty, "parent path must be nonempty")
    require(child.nonEmpty, "child path must be nonempty")
    "%s/%s" format(parent, child)
  }
}
