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

package kafka.manager.utils.zero81

import grizzled.slf4j.Logging
import kafka.common.TopicAndPartition
import kafka.manager.utils._
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.json4s.JsonAST._

/**
 * Borrowed from kafka 0.8.1.1, adapted to use curator framework
 * https://git-wip-us.apache.org/repos/asf?p=kafka.git;a=blob;f=core/src/main/scala/kafka/admin/PreferredReplicaLeaderElectionCommand.scala
 */
object PreferredReplicaLeaderElectionCommand extends Logging {

  def parsePreferredReplicaElectionData(jsonString: String): Set[TopicAndPartition] = {
    parseJson(jsonString).findField(_._1 == "partitions") match {
      case Some((_, arr)) =>
        val result: List[TopicAndPartition] = for {
          JArray(elements) <- arr
          JObject(children) <- elements
          JField("topic", JString(t)) <- children
          JField("partition", JInt(p)) <- children
        } yield TopicAndPartition(t, p.toInt)
        checkCondition(result.nonEmpty, PreferredLeaderElectionErrors.ElectionSetEmptyOnRead(jsonString))
        result.toSet
      case None =>
        throwError(PreferredLeaderElectionErrors.ElectionSetEmptyOnRead(jsonString))
    }
  }


  def writePreferredReplicaElectionData(curator: CuratorFramework,
                                        partitionsUndergoingPreferredReplicaElection: Set[TopicAndPartition]) {
    checkCondition(partitionsUndergoingPreferredReplicaElection.nonEmpty,PreferredLeaderElectionErrors.ElectionSetEmptyOnWrite)
    val zkPath = ZkUtils.PreferredReplicaLeaderElectionPath
    val partitionsList : Set[Map[String,Any]] =
      partitionsUndergoingPreferredReplicaElection.map(e => Map[String,Any]("topic" -> e.topic, "partition" -> e.partition))
    val jsonData = toJson(Map("version" -> 1, "partitions" -> partitionsList))
    try {
      ZkUtils.createPersistentPath(curator, zkPath, jsonData)
      logger.info("Created preferred replica election path with %s".format(jsonData))
    } catch {
      case nee: NodeExistsException =>
        val partitionsUndergoingPreferredReplicaElection =
          PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(ZkUtils.readData(curator, zkPath)._1)
        throwError(PreferredLeaderElectionErrors.ElectionAlreadyInProgress(partitionsUndergoingPreferredReplicaElection))
      case e2: Throwable =>
        throwError(PreferredLeaderElectionErrors.UnhandledException)
    }
  }
}

object PreferredLeaderElectionErrors {
  class ElectionSetEmptyOnWrite private[PreferredLeaderElectionErrors] extends UtilError("Preferred replica election data is empty")
  class ElectionSetEmptyOnRead private[PreferredLeaderElectionErrors] (json: String) extends UtilError(s"Preferred replica election data is empty on read : $json")
  class ElectionAlreadyInProgress private[PreferredLeaderElectionErrors] (partitionsUndergoingPreferredReplicaElection: Set[TopicAndPartition]) extends UtilError(
    "Preferred replica leader election currently in progress for " +
    "%s. Aborting operation".format(partitionsUndergoingPreferredReplicaElection))
  class UnhandledException private[PreferredLeaderElectionErrors] extends UtilError("Unhandled exception")

  def ElectionSetEmptyOnRead(json: String) = new ElectionSetEmptyOnRead(json)
  val ElectionSetEmptyOnWrite = new ElectionSetEmptyOnWrite
  def ElectionAlreadyInProgress(set: Set[TopicAndPartition]) = new ElectionAlreadyInProgress(set)
  val UnhandledException = new UnhandledException
}
