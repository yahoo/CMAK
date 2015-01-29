/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.manager.utils

import java.util.Properties

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Random

/**
 * Borrowed from kafka 0.8.1.1, adapted to use curator framework
 * https://git-wip-us.apache.org/repos/asf?p=kafka.git;a=blob;f=core/src/main/scala/kafka/admin/AdminUtils.scala
 */
object AdminUtils {

  private[this] lazy val logger = LoggerFactory.getLogger(this.getClass)

  val rand = new Random
  val TopicConfigChangeZnodePrefix = "config_change_"

  /**
   * There are 2 goals of replica assignment:
   * 1. Spread the replicas evenly among brokers.
   * 2. For partitions assigned to a particular broker, their other replicas are spread over the other brokers.
   *
   * To achieve this goal, we:
   * 1. Assign the first replica of each partition by round-robin, starting from a random position in the broker list.
   * 2. Assign the remaining replicas of each partition with an increasing shift.
   *
   * Here is an example of assigning
   * broker-0  broker-1  broker-2  broker-3  broker-4
   * p0        p1        p2        p3        p4       (1st replica)
   * p5        p6        p7        p8        p9       (1st replica)
   * p4        p0        p1        p2        p3       (2nd replica)
   * p8        p9        p5        p6        p7       (2nd replica)
   * p3        p4        p0        p1        p2       (3nd replica)
   * p7        p8        p9        p5        p6       (3nd replica)
   */
  def assignReplicasToBrokers(brokerList: Seq[Int],
                              nPartitions: Int,
                              replicationFactor: Int,
                              fixedStartIndex: Int = -1,
                              startPartitionId: Int = -1)
  : Map[Int, Seq[Int]] = {
    checkCondition(nPartitions > 0,TopicErrors.PartitionsGreaterThanZero)
    checkCondition(replicationFactor > 0,TopicErrors.ReplicationGreaterThanZero)
    checkCondition(replicationFactor <= brokerList.size,
      TopicErrors.ReplicationGreaterThanNumBrokers(replicationFactor, brokerList.size))

    val ret = new mutable.HashMap[Int, List[Int]]()
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size)
    var currentPartitionId = if (startPartitionId >= 0) startPartitionId else 0

    var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size)
    for (i <- 0 until nPartitions) {
      if (currentPartitionId > 0 && (currentPartitionId % brokerList.size == 0))
        nextReplicaShift += 1
      val firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size
      var replicaList = List(brokerList(firstReplicaIndex))
      for (j <- 0 until replicationFactor - 1)
        replicaList ::= brokerList(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size))
      ret.put(currentPartitionId, replicaList.reverse)
      currentPartitionId = currentPartitionId + 1
    }
    ret.toMap
  }

  private def replicaIndex(firstReplicaIndex: Int, secondReplicaShift: Int, replicaIndex: Int, nBrokers: Int): Int = {
    val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
    (firstReplicaIndex + shift) % nBrokers
  }

  def deleteTopic(curator: CuratorFramework, topic: String): Unit = {
    ZkUtils.createPersistentPath(curator,ZkUtils.getDeleteTopicPath(topic))
  }

  def createTopic(curator: CuratorFramework,
                  brokers: Seq[Int],
                  topic: String,
                  partitions: Int,
                  replicationFactor: Int,
                  topicConfig: Properties = new Properties): Unit = {

    val replicaAssignment = assignReplicasToBrokers(brokers,partitions,replicationFactor)
    createOrUpdateTopicPartitionAssignmentPathInZK(curator, topic, replicaAssignment, topicConfig)
  }

  def createOrUpdateTopicPartitionAssignmentPathInZK(curator: CuratorFramework,
                                                     topic: String,
                                                     partitionReplicaAssignment: Map[Int, Seq[Int]],
                                                     config: Properties = new Properties,
                                                     update: Boolean = false) {
    // validate arguments
    Topic.validate(topic)
    LogConfig.validate(config)
    checkCondition(partitionReplicaAssignment.values.map(_.size).toSet.size == 1, TopicErrors.InconsistentPartitionReplicas)

    val topicPath = ZkUtils.getTopicPath(topic)
    if(!update ) {
      checkCondition(curator.checkExists().forPath(topicPath) == null,TopicErrors.TopicAlreadyExists(topic))
    }
    partitionReplicaAssignment.foreach {
      case (part,reps) => checkCondition(reps.size == reps.toSet.size, TopicErrors.DuplicateReplicAssignment(topic,part,reps))
    }

    // write out the config if there is any, this isn't transactional with the partition assignments
    writeTopicConfig(curator, topic, config)

    // create the partition assignment
    writeTopicPartitionAssignment(curator, topic, partitionReplicaAssignment, update)
  }

  /**
   * Write out the topic config to zk, if there is any
   */
  private def writeTopicConfig(curator: CuratorFramework, topic: String, config: Properties) {
    val configMap: mutable.Map[String, String] = {
      import scala.collection.JavaConverters._
      config.asScala
    }
    val map : Map[String, Any] = Map("version" -> 1, "config" -> configMap)
    ZkUtils.updatePersistentPath(curator, ZkUtils.getTopicConfigPath(topic), toJson(map))
  }

  private def writeTopicPartitionAssignment(curator: CuratorFramework, topic: String, replicaAssignment: Map[Int, Seq[Int]], update: Boolean) {
    try {
      val zkPath = ZkUtils.getTopicPath(topic)
      val jsonPartitionData = ZkUtils.replicaAssignmentZkData(replicaAssignment.map(e => (e._1.toString -> e._2)))

      if (!update) {
        logger.info("Topic creation {}", jsonPartitionData.toString)
        ZkUtils.createPersistentPath(curator, zkPath, jsonPartitionData)
      } else {
        logger.info("Topic update {}", jsonPartitionData.toString)
        ZkUtils.updatePersistentPath(curator, zkPath, jsonPartitionData)
      }
      logger.debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionData))
    } catch {
      case e: NodeExistsException => throw new IllegalArgumentException("topic %s already exists".format(topic))
      case e2: Throwable => throw new IllegalArgumentException(e2.toString)
    }
  }
}
