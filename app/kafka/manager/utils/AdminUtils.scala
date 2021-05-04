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

import grizzled.slf4j.Logging
import kafka.manager.model._
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.NodeExistsException

import java.util.Properties
import scala.collection.{Set, mutable}
import scala.util.Random

/**
 * Borrowed from kafka 0.8.1.1, adapted to use curator framework
 * https://git-wip-us.apache.org/repos/asf?p=kafka.git;a=blob;f=core/src/main/scala/kafka/admin/AdminUtils.scala
 */
class AdminUtils(version: KafkaVersion) extends Logging {

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
  def assignReplicasToBrokers(brokerListSet: Set[Int],
                              nPartitions: Int,
                              replicationFactor: Int,
                              fixedStartIndex: Int = -1,
                              startPartitionId: Int = -1)
  : Map[Int, Seq[Int]] = {
    val brokerList : Seq[Int] = brokerListSet.toSeq.sorted
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
    checkCondition(topicExists(curator, topic),TopicErrors.TopicDoesNotExist(topic))
    ZkUtils.createPersistentPath(curator,ZkUtils.getDeleteTopicPath(topic))
  }

  def createTopic(curator: CuratorFramework,
                  brokers: Set[Int],
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
                                                     update: Boolean = false, 
                                                     readVersion: Int = -1) {
    // validate arguments
    Topic.validate(topic)
    TopicConfigs.validate(version,config)
    checkCondition(partitionReplicaAssignment.values.map(_.size).toSet.size == 1, TopicErrors.InconsistentPartitionReplicas)

    val topicPath = ZkUtils.getTopicPath(topic)
    if(!update ) {
      checkCondition(curator.checkExists().forPath(topicPath) == null,TopicErrors.TopicAlreadyExists(topic))
    }
    partitionReplicaAssignment.foreach {
      case (part,reps) => checkCondition(reps.size == reps.toSet.size, TopicErrors.DuplicateReplicaAssignment(topic,part,reps))
    }

    // write out the config on create, not update, if there is any, this isn't transactional with the partition assignments
    if(!update) {
      writeTopicConfig(curator, topic, config)
    }

    // create the partition assignment
    writeTopicPartitionAssignment(curator, topic, partitionReplicaAssignment, update, readVersion)
  }

  /**
   * Write out the topic config to zk, if there is any
   */
  private def writeTopicConfig(curator: CuratorFramework, topic: String, config: Properties, readVersion: Int = -1) {
    val configMap: mutable.Map[String, String] = {
      import scala.collection.JavaConverters._
      config.asScala
    }
    val map : Map[String, Any] = Map("version" -> 1, "config" -> configMap)
    ZkUtils.updatePersistentPath(curator, ZkUtils.getTopicConfigPath(topic), toJson(map), readVersion)
  }

  private def writeTopicPartitionAssignment(curator: CuratorFramework, 
                                            topic: String, 
                                            replicaAssignment: Map[Int, Seq[Int]], 
                                            update: Boolean, 
                                            readVersion: Int = -1) {
    try {
      val zkPath = ZkUtils.getTopicPath(topic)
      val jsonPartitionData = ZkUtils.replicaAssignmentZkData(replicaAssignment.map(e => (e._1.toString -> e._2)))

      if (!update) {
        logger.info(s"Topic creation ${jsonPartitionData.toString}")
        ZkUtils.createPersistentPath(curator, zkPath, jsonPartitionData)
      } else {
        logger.info(s"Topic update ${jsonPartitionData.toString}")
        ZkUtils.updatePersistentPath(curator, zkPath, jsonPartitionData, readVersion)
      }
      logger.debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionData))
    } catch {
      case e: NodeExistsException => throw new IllegalArgumentException("topic %s already exists".format(topic))
      case e2: Throwable => throw new IllegalArgumentException(e2.toString)
    }
  }

  /**
   * Add partitions to existing topic with optional replica assignment
   *
   * @param curator Zookeeper client
   * @param topic topic for adding partitions to
   * @param newNumPartitions Number of partitions to be set
   * @param partitionReplicaList current partition to replic set mapping
   * @param brokerList broker list
   */
  def addPartitions(curator: CuratorFramework,
                    topic: String,
                    newNumPartitions: Int,
                    partitionReplicaList : Map[Int, Seq[Int]],
                    brokerList: Set[Int],
                    readVersion: Int) {
    
    /*
    import collection.JavaConverters._
    val newConfigSet = config.entrySet().asScala.map(e => (e.getKey.toString, e.getValue.toString)).toSet

    if(newConfigSet == oldConfigSet) {
      logger.info(s"No config changes.  newConfigSet=$newConfigSet oldConfigSet=$oldConfigSet")
    } else {
      logger.info(s"Config changed.  newConfigSet=$newConfigSet oldConfigSet=$oldConfigSet")
      changeTopicConfig(curator,topic,config)
    }*/
    
    val brokerListSorted: Set[Int] = brokerList
    val currentNumPartitions: Int = partitionReplicaList.size

    checkCondition(currentNumPartitions > 0,
      TopicErrors.PartitionsGreaterThanZero)
    
    checkCondition(currentNumPartitions < newNumPartitions,
      TopicErrors.CannotAddZeroPartitions(topic,currentNumPartitions,newNumPartitions))

    val currentReplicationFactor: Int = partitionReplicaList.head._2.size
    
    checkCondition(brokerListSorted.size >= currentReplicationFactor,
      TopicErrors.ReplicationGreaterThanNumBrokers(currentReplicationFactor,brokerListSorted.size))
    
    val partitionsToAdd = newNumPartitions - currentNumPartitions

    // create the new partition replication list
    val addedPartitionReplicaList : Map[Int, Seq[Int]] =
      assignReplicasToBrokers(
        brokerListSorted, partitionsToAdd, currentReplicationFactor, partitionReplicaList.head._2.head, currentNumPartitions)

    logger.info("Add partition list for %s is %s".format(topic, addedPartitionReplicaList))
    //val partitionReplicaList : Map[Int, Seq[Int]] = topicIdentity.partitionsIdentity.map(p => p._1 -> p._2.replicas.toSeq)
    
    // add the new partitions
    val newPartitionsReplicaList : Map[Int, Seq[Int]] = partitionReplicaList ++ addedPartitionReplicaList
    
    checkCondition(newPartitionsReplicaList.size == newNumPartitions,
      TopicErrors.FailedToAddNewPartitions(topic, newNumPartitions, newPartitionsReplicaList.size))
    
    createOrUpdateTopicPartitionAssignmentPathInZK(curator, topic, newPartitionsReplicaList, update=true, readVersion=readVersion)
  }

  /* Add partitions to multiple topics. After this operation, all topics will have the same number of partitions */
  def addPartitionsToTopics(curator: CuratorFramework,
                            topicAndReplicaList: Seq[(String, Map[Int, Seq[Int]])],
                            newNumPartitions: Int,
                            brokerList: Set[Int],
                            readVersions: Map[String,Int]) {
    val topicsWithoutReadVersion = topicAndReplicaList.map(x=>x._1).filter{t => !readVersions.contains(t)}
    checkCondition(topicsWithoutReadVersion.isEmpty, TopicErrors.NoReadVersionFound(topicsWithoutReadVersion.mkString(", ")))

    // topicAndReplicaList is sorted by number of partitions each topic has in order not to start adding partitions if any of requests doesn't work with newNumPartitions
    for {
      (topic, replicaList) <- topicAndReplicaList
      readVersion = readVersions(topic)
    } {
      addPartitions(curator, topic, newNumPartitions, replicaList, brokerList, readVersion)
    }
  }

  /**
   * Update the config for an existing topic and create a change notification so the change will propagate to other brokers
   * @param curator: The zk client handle used to write the new config to zookeeper
   * @param topic: The topic for which configs are being changed
   * @param config: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
  def changeTopicConfig(curator: CuratorFramework, topic: String, config: Properties, readVersion: Int) {
    checkCondition(topicExists(curator, topic),TopicErrors.TopicDoesNotExist(topic))

    // remove the topic overrides
    TopicConfigs.validate(version,config)

    // write the new config--may not exist if there were previously no overrides
    writeTopicConfig(curator, topic, config, readVersion)

    // Create the topic change data
    val topicChange = version match {
      case Kafka_0_8_1_1 | Kafka_0_8_2_0 | Kafka_0_8_2_1 | Kafka_0_8_2_2 => toJson(topic)
      case _ => toJson(Map(
        "version" -> 1,
        "entity_type" -> "topics",
        "entity_name" -> topic
      ))
    }

    // create the change notification
    curator
      .create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
      .forPath(s"${ZkUtils.TopicConfigChangesPath}/$TopicConfigChangeZnodePrefix", topicChange)
  }
  
  def topicExists(curator: CuratorFramework, topic: String):  Boolean = {
    val topicPath = ZkUtils.getTopicPath(topic)
    val result = curator.checkExists().forPath(topicPath)
    result != null
  }
}
