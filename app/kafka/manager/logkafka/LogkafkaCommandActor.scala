/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.logkafka

import kafka.manager.base.{BaseCommandActor, LongRunningPoolActor, LongRunningPoolConfig}
import kafka.manager.model.ActorModel._
import kafka.manager.model.ClusterContext
import kafka.manager.utils.LogkafkaAdminUtils
import org.apache.curator.framework.CuratorFramework

import scala.concurrent.Future
import scala.util.Try

/**
 * @author hiral
 */

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
      case LKCDeleteLogkafka(logkafka_id, log_path, logkafkaConfig) =>
        longRunning {
          Future {
            LKCCommandResult(Try {
              logkafkaAdminUtils.deleteLogkafka(logkafkaCommandActorConfig.curator, logkafka_id, log_path, logkafkaConfig)
            })
          }
        }
      case LKCCreateLogkafka(logkafka_id, log_path, config, logkafkaConfig) =>
        longRunning {
          Future {
            LKCCommandResult(Try {
              logkafkaAdminUtils.createLogkafka(logkafkaCommandActorConfig.curator, logkafka_id, log_path, config, logkafkaConfig)
            })
          }
        }
      case LKCUpdateLogkafkaConfig(logkafka_id, log_path, config, logkafkaConfig, checkConfig) =>
        longRunning {
          Future {
            LKCCommandResult(Try {
              logkafkaAdminUtils.changeLogkafkaConfig(logkafkaCommandActorConfig.curator, logkafka_id, log_path, config, logkafkaConfig, checkConfig)
            })
          }
        }
      case any: Any => log.warning("lkca : processCommandRequest : Received unknown message: {}", any)
    }
  }
}

