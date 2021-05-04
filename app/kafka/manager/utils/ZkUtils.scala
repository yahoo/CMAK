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

import org.apache.curator.framework.CuratorFramework
import org.apache.kafka.common.TopicPartition
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.{NoNodeException, NodeExistsException}
import org.apache.zookeeper.data.Stat

import java.nio.charset.StandardCharsets

/**
 * Borrowed from kafka 0.8.1.1.  Adapted to use curator framework.
 * https://git-wip-us.apache.org/repos/asf?p=kafka.git;a=blob;f=core/src/main/scala/kafka/utils/ZkUtils.scala
 */
object ZkUtils {

  val ConsumersPath = "/consumers"
  val BrokerIdsPath = "/brokers/ids"
  val BrokerTopicsPath = "/brokers/topics"
  val TopicConfigPath = "/config/topics"
  val TopicConfigChangesPath = "/config/changes"
  val ControllerPath = "/controller"
  val ControllerEpochPath = "/controller_epoch"
  val ReassignPartitionsPath = "/admin/reassign_partitions"
  val DeleteTopicsPath = "/admin/delete_topics"
  val PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election"
  val AdminPath = "/admin"
  val SchedulePreferredLeaderElectionPath = AdminPath + "/schedule_leader_election"

  def getTopicPath(topic: String): String = {
    BrokerTopicsPath + "/" + topic
  }

  def getTopicPartitionsPath(topic: String): String = {
    getTopicPath(topic) + "/partitions"
  }

  def getTopicConfigPath(topic: String): String =
    TopicConfigPath + "/" + topic

  def getDeleteTopicPath(topic: String): String =
    DeleteTopicsPath + "/" + topic

  implicit def serializeString(str: String): Array[Byte] = {
    str.getBytes(StandardCharsets.UTF_8)
  }

  /**
   * Update the value of a persistent node with the given path and data.
   * create parent directory if necessary. Never throw NodeExistException.
   * Return the updated path zkVersion
   */
  def updatePersistentPath(curator: CuratorFramework, path: String, ba: Array[Byte], version: Int = -1) = {
    try {
      curator.setData().withVersion(version).forPath(path, ba)
    } catch {
      case e: NoNodeException => {
        try {
          createPersistentPath(curator, path, ba)
        } catch {
          case e: NodeExistsException =>
            curator.setData().forPath(path, ba)
          case e2: Throwable => throw e2
        }
      }
      case e2: Throwable => throw e2
    }
  }

  def createPersistentPath(curator: CuratorFramework, path: String): Unit = {
    curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path)
  }

  /**
   * Create a persistent node with the given path and data. Create parents if necessary.
   */
  def createPersistentPath(curator: CuratorFramework, path: String, data: Array[Byte]): Unit = {
    curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, data)
  }

  /**
   * Get JSON partition to replica map from zookeeper.
   */
  def replicaAssignmentZkData(map: Map[String, Seq[Int]]): String = {
    toJson(Map("version" -> 1, "partitions" -> map))
  }

  def readData(curator: CuratorFramework, path: String): (String, Stat) = {
    val stat: Stat = new Stat()
    val dataStr: String = curator.getData.storingStatIn(stat).forPath(path)
    (dataStr, stat)
  }
  
  def readDataMaybeNull(curator: CuratorFramework, path: String): (Option[String], Stat) = {
    val stat: Stat = new Stat()
    try {
      val dataStr: String = curator.getData.storingStatIn(stat).forPath(path)
      (Option(dataStr), stat)
    } catch {
      case e: NoNodeException => {
        (None, stat)
      }
      case e2: Throwable => throw e2
    }
  }


  def getPartitionReassignmentZkData(partitionsToBeReassigned: Map[TopicPartition, Seq[Int]]): String = {
    toJson(Map("version" -> 1, "partitions" -> partitionsToBeReassigned.map(e => Map("topic" -> e._1.topic, "partition" -> e._1.partition,
      "replicas" -> e._2))))
  }
}
