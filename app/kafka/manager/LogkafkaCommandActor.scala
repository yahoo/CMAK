/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit, ThreadPoolExecutor}

import akka.pattern._
import akka.util.Timeout
import kafka.manager.features.KMDeleteTopicFeature
import org.apache.curator.framework.CuratorFramework
import kafka.manager.utils.{LogkafkaAdminUtils, ZkUtils}

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Try}

/**
 * @author hiral
 */

import ActorModel._

case class LogkafkaCommandActorConfig(curator: CuratorFramework, 
                                   longRunningPoolConfig: LongRunningPoolConfig,
                                   askTimeoutMillis: Long = 400, 
                                   clusterContext: ClusterContext)
class LogkafkaCommandActor(logkafkaCommandActorConfig: LogkafkaCommandActorConfig) extends BaseCommandActor with LongRunningPoolActor {

  //private[this] val askTimeout: Timeout = logkafkaCommandActorConfig.askTimeoutMillis.milliseconds

  private[this] val logkafkaAdminUtils = new LogkafkaAdminUtils(logkafkaCommandActorConfig.clusterContext.config.version)

  @scala.throws[Exception](classOf[Exception])
  override def preStart() = {
    log.info("Started actor %s".format(self.path))
  }

  @scala.throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]) {
    log.error(reason, "Restarting due to [{}] when processing [{}]",
      reason.getMessage, message.getOrElse(""))
    super.preRestart(reason, message)
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
  }

  override protected def longRunningPoolConfig: LongRunningPoolConfig = logkafkaCommandActorConfig.longRunningPoolConfig

  override protected def longRunningQueueFull(): Unit = {
    sender ! LKCCommandResult(Try(throw new UnsupportedOperationException("Long running executor blocking queue is full!")))
  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case any: Any => log.warning("lkca : processActorResponse : Received unknown message: {}", any)
    }
  }

  override def processCommandRequest(request: CommandRequest): Unit = {
    implicit val ec = longRunningExecutionContext
    request match {
      case LKCDeleteLogkafka(hostname, log_path, logkafkaConfig) =>
        if(logkafkaCommandActorConfig.clusterContext.clusterFeatures.features(KMDeleteTopicFeature)) {
          longRunning {
            Future {
              LKCCommandResult(Try {
                logkafkaAdminUtils.deleteLogkafka(logkafkaCommandActorConfig.curator, hostname, log_path, logkafkaConfig)
              })
            }
          }
        } else {
          val result : LKCCommandResult = LKCCommandResult(Failure(new UnsupportedOperationException(
            s"Delete logkafka not supported for kafka version ${logkafkaCommandActorConfig.clusterContext.config.version}")))
          sender ! result
        }
      case LKCCreateLogkafka(hostname, log_path, config, logkafkaConfig) =>
        longRunning {
          Future {
            LKCCommandResult(Try {
              logkafkaAdminUtils.createLogkafka(logkafkaCommandActorConfig.curator, hostname, log_path, config, logkafkaConfig)
            })
          }
        }
      case LKCUpdateLogkafkaConfig(hostname, log_path, config, logkafkaConfig) =>
        longRunning {
          Future {
            LKCCommandResult(Try {
              logkafkaAdminUtils.changeLogkafkaConfig(logkafkaCommandActorConfig.curator, hostname, log_path, config, logkafkaConfig)
            })
          }
        }
      case any: Any => log.warning("lkca : processCommandRequest : Received unknown message: {}", any)
    }
  }
}

