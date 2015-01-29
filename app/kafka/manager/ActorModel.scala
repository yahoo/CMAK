/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import java.util.Properties

import org.joda.time.DateTime
import kafka.manager.utils.TopicAndPartition

import scala.util.Try

/**
 * @author hiral
 */
object ActorModel {
  sealed trait ActorRequest
  sealed trait ActorResponse

  sealed trait CommandRequest extends ActorRequest
  sealed trait CommandResponse extends ActorResponse

  sealed trait QueryRequest extends ActorRequest
  sealed trait QueryResponse extends ActorResponse

  case class ActorErrorResponse(msg: String, throwableOption: Option[Throwable] = None) extends ActorResponse

  sealed trait BVRequest extends QueryRequest

  case object BVForceUpdate extends CommandRequest
  case class BVGetView(id: Int) extends BVRequest
  case class BVView(topicPartitions: Map[TopicIdentity, IndexedSeq[Int]]) extends QueryResponse

  case object CMGetView extends QueryRequest
  case class CMView(topicsCount: Int, brokersCount: Int, clusterConfig: ClusterConfig) extends QueryResponse
  case object CMShutdown extends CommandRequest
  case class CMCreateTopic(topic: String,
                           partitions: Int,
                           replicationFactor: Int,
                           config: Properties = new Properties) extends CommandRequest
  case class CMDeleteTopic(topic: String) extends CommandRequest
  case class CMRunPreferredLeaderElection(topics: Set[String]) extends CommandRequest
  case class CMRunReassignPartition(topics: Set[String]) extends CommandRequest
  case class CMGeneratePartitionAssignments(topics: Set[String]) extends CommandRequest


  case class CMCommandResult(result: Try[Unit]) extends CommandResponse
  case class CMCommandResults(result: IndexedSeq[Try[Unit]]) extends CommandResponse

  case class KCCreateTopic(topic: String,
                           brokers: Seq[Int],
                           partitions: Int,
                           replicationFactor:Int,
                           config: Properties) extends CommandRequest
  case class KCDeleteTopic(topic: String) extends CommandRequest
  case class KCPreferredReplicaLeaderElection(topicAndPartition: Set[TopicAndPartition]) extends CommandRequest
  case class KCReassignPartition(currentTopicIdentity: Map[String, TopicIdentity],
                               generatedTopicIdentity: Map[String, TopicIdentity]) extends CommandRequest

  case class KCCommandResult(result: Try[Unit]) extends CommandResponse

  case object KMGetActiveClusters extends QueryRequest
  case object KMGetAllClusters extends QueryRequest
  case class KMGetClusterConfig(clusterName: String) extends QueryRequest
  case class KMClusterQueryRequest(clusterName: String, request: QueryRequest) extends QueryRequest
  case class KMQueryResult(result: IndexedSeq[ClusterConfig]) extends QueryResponse
  case class KMClusterConfigResult(result: Try[ClusterConfig]) extends QueryResponse
  case class KMClusterList(active: IndexedSeq[ClusterConfig], pending : IndexedSeq[ClusterConfig]) extends QueryResponse

  case object KMUpdateState extends CommandRequest
  case object KMPruneClusters extends CommandRequest
  case object KMShutdown extends CommandRequest
  case object KMShutdownComplete extends CommandResponse
  case class KMAddCluster(config: ClusterConfig) extends CommandRequest
  case class KMUpdateCluster(config: ClusterConfig) extends CommandRequest
  case class KMEnableCluster(clusterName: String) extends CommandRequest
  case class KMDisableCluster(clusterName: String) extends CommandRequest
  case class KMDeleteCluster(clusterName: String) extends CommandRequest
  case class KMClusterCommandRequest(clusterName: String, request: CommandRequest) extends CommandRequest
  case class KMCommandResult(result: Try[Unit]) extends CommandResponse

  sealed trait KSRequest extends QueryRequest
  case object KSGetTopics extends KSRequest
  case class KSGetTopicConfig(topic: String) extends KSRequest
  case class KSGetTopicDescription(topic: String) extends KSRequest
  case class KSGetAllTopicDescriptions(lastUpdateMillis: Option[Long]= None) extends KSRequest
  case class KSGetTopicDescriptions(topics: Set[String]) extends KSRequest
  case object KSGetTopicsLastUpdateMillis extends KSRequest
  case object KSGetPreferredLeaderElection extends KSRequest
  case object KSGetReassignPartition extends KSRequest
  case class KSEndPreferredLeaderElection(millis: Long) extends CommandRequest
  case class KSUpdatePreferredLeaderElection(millis: Long, json: String) extends CommandRequest
  case class KSEndReassignPartition(millis: Long) extends CommandRequest
  case class KSUpdateReassignPartition(millis: Long, json: String) extends CommandRequest

  case object KSGetBrokers extends KSRequest
  case class KSGetBrokerState(id: String) extends  KSRequest

  case class TopicList(list: IndexedSeq[String]) extends QueryResponse
  case class TopicConfig(topic: String, config: Option[String]) extends QueryResponse

  case class TopicDescription(topic: String, description: String, partitionState: Option[Map[String, String]]) extends  QueryResponse
  case class TopicDescriptions(descriptions: IndexedSeq[TopicDescription], lastUpdateMillis: Long) extends QueryResponse

  case class BrokerInfo(id: String, config: String)
  case class BrokerList(list: IndexedSeq[BrokerInfo]) extends QueryResponse

  case class PreferredReplicaElection(startTime: DateTime, topicAndPartition: Set[TopicAndPartition], endTime: Option[DateTime]) extends QueryResponse
  case class ReassignPartitions(startTime: DateTime, partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]], endTime: Option[DateTime]) extends QueryResponse

  case object DCUpdateState extends CommandRequest
}
