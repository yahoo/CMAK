package scheduler.kafka.manager

import kafka.manager.ActorModel._
import kafka.manager._
import org.apache.curator.framework.CuratorFramework

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class KafkaSchedulerCommandActorConfig(schedulerConfig: SchedulerConfig,
                                            curator: CuratorFramework,
                                            longRunningPoolConfig: LongRunningPoolConfig,
                                            askTimeoutMillis: Long = 400,
                                            version: KafkaVersion)

class KafkaSchedulerCommandActor(kafkaCommandActorConfig: KafkaSchedulerCommandActorConfig) extends BaseCommandActor with LongRunningPoolActor {

  val schedulerRestClient = new SchedulerRestClient(kafkaCommandActorConfig.schedulerConfig.apiUrl)(play.api.libs.concurrent.Execution.Implicits.defaultContext)

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

  override protected def longRunningPoolConfig: LongRunningPoolConfig = kafkaCommandActorConfig.longRunningPoolConfig

  override protected def longRunningQueueFull(): Unit = {
    sender ! KCCommandResult(Try(throw new UnsupportedOperationException("Long running executor blocking queue is full!")))
  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case any: Any => log.warning("ksca : processActorResponse : Received unknown message: {}", any)
    }
  }

  def futureToKCCommandResult[T](future: Future[T])(implicit ec: ExecutionContext): Future[KCCommandResult] = {
    future.map {
      _ => KCCommandResult(Success(()))
    }.recover {
      case e: Throwable => KCCommandResult(Failure(e))
    }
  }

  override def processCommandRequest(request: CommandRequest): Unit = {
    implicit val ec = longRunningExecutionContext
    request match {

      case KSCAddBroker(id, cpus, mem, heap, port, bindAddress, constraints, options, log4jOptions, jvmOptions, stickinessPeriod, failover) =>
        longRunning {
          futureToKCCommandResult(schedulerRestClient.addBroker(id, cpus, mem, heap, port, bindAddress, constraints, options, log4jOptions, jvmOptions, stickinessPeriod, failover))
        }

      case KSCUpdateBroker(id, cpus, mem, heap, port, bindAddress, constraints, options, log4jOptions, jvmOptions, stickinessPeriod, failover) =>
        longRunning {
          futureToKCCommandResult(schedulerRestClient.updateBroker(id, cpus, mem, heap, port, bindAddress, constraints, options, log4jOptions, jvmOptions, stickinessPeriod, failover))
        }

      case KSCStartBroker(id) =>
        longRunning {
          futureToKCCommandResult(schedulerRestClient.startBroker(id))
        }

      case KSCStopBroker(id) =>
        longRunning {
          futureToKCCommandResult(schedulerRestClient.stopBroker(id))
        }

      case KSCRemoveBroker(id) =>
        longRunning {
          futureToKCCommandResult(schedulerRestClient.removeBroker(id))
        }

      case KSCRebalanceTopics(ids, topics) =>
        longRunning {
          futureToKCCommandResult(schedulerRestClient.rebalanceTopics(ids, topics))
        }

      case any: Any => log.warning("ksca : processCommandRequest : Received unknown message: {}", any)
    }
  }
}


