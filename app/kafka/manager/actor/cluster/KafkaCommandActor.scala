/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.actor.cluster

import kafka.manager.base.cluster.BaseClusterCommandActor
import kafka.manager.base.{LongRunningPoolActor, LongRunningPoolConfig}
import kafka.manager.features.KMDeleteTopicFeature
import kafka.manager.model.ActorModel._
import kafka.manager.model.ClusterContext
import kafka.manager.utils.AdminUtils
import kafka.manager.utils.zero81.{PreferredReplicaLeaderElectionCommand, ReassignPartitionCommand}
import org.apache.curator.framework.CuratorFramework

import scala.concurrent.Future
import scala.util.{Failure, Try}

/**
 * @author hiral
 */

case class KafkaCommandActorConfig(curator: CuratorFramework, 
                                   longRunningPoolConfig: LongRunningPoolConfig,
                                   askTimeoutMillis: Long = 400, 
                                   clusterContext: ClusterContext, 
                                   adminUtils: AdminUtils)
class KafkaCommandActor(kafkaCommandActorConfig: KafkaCommandActorConfig) extends BaseClusterCommandActor with LongRunningPoolActor {

  protected implicit val clusterContext: ClusterContext = kafkaCommandActorConfig.clusterContext
  //private[this] val askTimeout: Timeout = kafkaCommandActorConfig.askTimeoutMillis.milliseconds

  private[this] val reassignPartitionCommand = new ReassignPartitionCommand(kafkaCommandActorConfig.adminUtils)
  
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
      case any: Any => log.warning("kca : processActorResponse : Received unknown message: {}", any)
    }
  }

  override def processCommandRequest(request: CommandRequest): Unit = {
    implicit val ec = longRunningExecutionContext
    request match {
      case KCDeleteTopic(topic) =>
        featureGateFold(KMDeleteTopicFeature)(
        {
          val result : KCCommandResult = KCCommandResult(Failure(new UnsupportedOperationException(
            s"Delete topic not supported for kafka version ${kafkaCommandActorConfig.clusterContext.config.version}")))
          sender ! result
        },
        {
          longRunning {
            Future {
              KCCommandResult(Try {
                log.info(s"Deleting topic : $topic")
                kafkaCommandActorConfig.adminUtils.deleteTopic(kafkaCommandActorConfig.curator, topic) //this should work in 0.8.2
              })
            }
          }
        })
      case KCCreateTopic(topic, brokers, partitions, replicationFactor, config) =>
        longRunning {
          Future {
            KCCommandResult(Try {
              kafkaCommandActorConfig.adminUtils.createTopic(kafkaCommandActorConfig.curator, brokers, topic, partitions, replicationFactor, config)
            })
          }
        }
      case KCAddTopicPartitions(topic, brokers, partitions, partitionReplicaList, readVersion) =>
        longRunning {
          Future {
            KCCommandResult(Try {
              kafkaCommandActorConfig.adminUtils.addPartitions(kafkaCommandActorConfig.curator, topic, partitions, partitionReplicaList, brokers, readVersion)
            })
          }
        }
      case KCAddMultipleTopicsPartitions(topicsAndReplicas, brokers, partitions, readVersion) =>
        longRunning {
          Future {
            KCCommandResult(Try {
              kafkaCommandActorConfig.adminUtils.addPartitionsToTopics(kafkaCommandActorConfig.curator, topicsAndReplicas, partitions, brokers, readVersion)
            })
          }
        }
      case KCUpdateTopicConfig(topic, config, readVersion) =>
        longRunning {
          Future {
            KCCommandResult(Try {
              kafkaCommandActorConfig.adminUtils.changeTopicConfig(kafkaCommandActorConfig.curator, topic, config, readVersion)
            })
          }
        }
      case KCPreferredReplicaLeaderElection(topicAndPartition) =>
        longRunning {
          log.info("Running replica leader election : {}", topicAndPartition)
          Future {
            KCCommandResult(
              Try {
                PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(kafkaCommandActorConfig.curator, topicAndPartition)
              }
            )
          }
        }
      case KCReassignPartition(current, generated, forceSet) =>
        longRunning {
          log.info("Running reassign partition from {} to {}", current, generated)
          Future {
            KCCommandResult(
              reassignPartitionCommand.executeAssignment(kafkaCommandActorConfig.curator, current, generated, forceSet)
            )
          }
        }
      case any: Any => log.warning("kca : processCommandRequest : Received unknown message: {}", any)
    }
  }
}

