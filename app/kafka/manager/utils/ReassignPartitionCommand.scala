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

import kafka.manager.{TopicPartitionIdentity, TopicIdentity}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.slf4j.LoggerFactory

import scala.util.Try

/**
 * Borrowed from kafka 0.8.1.1, adapted to use curator framework
 * https://git-wip-us.apache.org/repos/asf?p=kafka.git;a=blob;f=core/src/main/scala/kafka/admin/ReassignPartitionsCommand.scala
 */
object ReassignPartitionCommand {

  import ReassignPartitionErrors._

  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  def generateAssignment(brokerList: Seq[Int], currentTopicIdentity : TopicIdentity) : Try[TopicIdentity] = {
    Try {
      val assignedReplicas = AdminUtils.assignReplicasToBrokers(
        brokerList,
        currentTopicIdentity.partitions,
        currentTopicIdentity.replicationFactor)
      val newTpi : Map[Int, TopicPartitionIdentity] = currentTopicIdentity.partitionsIdentity.map { case (part, tpi) =>
        val newReplicaSet = assignedReplicas.get(part)
        checkCondition(newReplicaSet.isDefined, MissingReplicaSetForPartition(part))
        (part,tpi.copy(replicas = newReplicaSet.get.toSet))
      }
      logger.info(s"Generated topic replica assignment topic=${currentTopicIdentity.topic}, $newTpi")
      TopicIdentity(currentTopicIdentity.topic,currentTopicIdentity.partitions,newTpi,currentTopicIdentity.numBrokers)
    }
  }

  def validateAssignment(current: TopicIdentity, generated: TopicIdentity) : Unit = {
    //perform validation

    checkCondition(generated.partitionsIdentity.nonEmpty, ReassignmentDataEmptyForTopic(current.topic))
    checkCondition(current.partitions == generated.partitions, PartitionsOutOfSync(current.partitions,generated.partitions))
    checkCondition(current.replicationFactor == generated.replicationFactor, ReplicationOutOfSync(current.replicationFactor, generated.replicationFactor))
  }

  def getValidAssignments(currentTopicIdentity: Map[String, TopicIdentity],
                          generatedTopicIdentity: Map[String, TopicIdentity]) : Try[Map[TopicAndPartition, Seq[Int]]] = {
    Try {
      currentTopicIdentity.flatMap { case (topic, current) =>
        generatedTopicIdentity.get(topic).fold {
          logger.info(s"No generated assignment found for topic=$topic, skipping")
          Map.empty[TopicAndPartition, Seq[Int]]
        } { generated =>
          validateAssignment(current, generated)
          for {
          //match up partitions from current to generated
            (currentPart, currentTpi) <- current.partitionsIdentity
            generatedTpi <- generated.partitionsIdentity.get(currentPart)

          } yield {
            logger.info("Reassigning replicas for topic=%s, partition=%s,  current=%s, generated=%s"
              .format(topic, currentPart, current.partitionsIdentity, generated.partitionsIdentity))
            (TopicAndPartition(topic, currentPart), generatedTpi.replicas.toSeq)
          }
        }
      }
    }
  }

  def executeAssignment(curator: CuratorFramework,
                        currentTopicIdentity: Map[String, TopicIdentity],
                        generatedTopicIdentity: Map[String, TopicIdentity]) : Try[Unit] = {
    getValidAssignments(currentTopicIdentity, generatedTopicIdentity).flatMap {
      validAssignments =>
        Try {
          checkCondition(validAssignments.nonEmpty, NoValidAssignments)
          val jsonReassignmentData = ZkUtils.getPartitionReassignmentZkData(validAssignments)
          try {
            logger.info(s"Creating reassign partitions path ${ZkUtils.ReassignPartitionsPath} : $jsonReassignmentData")
            //validate parsing of generated json
            parsePartitionReassignmentZkData(jsonReassignmentData)
            ZkUtils.createPersistentPath(curator, ZkUtils.ReassignPartitionsPath, jsonReassignmentData)
          } catch {
            case ze: NodeExistsException =>
              throwError(AlreadyInProgress)
            case e: Throwable =>
              throwError(FailedToReassignPartitionReplicas(e))
          }
        }
    }
  }

  def parsePartitionReassignmentZkData(json : String) : Map[TopicAndPartition, Seq[Int]] = {
    import org.json4s.JsonAST._
    parseJson(json).findField(_._1 == "partitions") match {
      case Some((_, arr)) =>
        val result : List[(TopicAndPartition, Seq[Int])] = for {
          JArray(elements) <- arr
          JObject(children) <- elements
          JField("topic", JString(t)) <- children
          JField("partition", JInt(i)) <- children
          JField("replicas", arr2) <- children
          JArray(assignments) <- arr2
        } yield (TopicAndPartition(t,i.toInt),assignments.map(_.extract[Int]))
        checkCondition(result.nonEmpty, NoValidAssignments)
        result.foreach { case (tAndP, a) =>
          checkCondition(a.nonEmpty, ReassignmentDataEmptyForTopic(tAndP.topic))
        }
        result.toMap
      case None =>
        throwError(NoValidAssignments)
    }
  }
}

object ReassignPartitionErrors {

  class MissingReplicaSetForPartition private[ReassignPartitionErrors](part: Int) extends UtilError(s"Failed to find new replica set for partition $part")
  class ReassignmentDataEmptyForTopic private[ReassignPartitionErrors](topic: String) extends UtilError(s"Partition reassignment data is empty for topic $topic")
  class PartitionsOutOfSync private[ReassignPartitionErrors](current: Int, generated: Int) extends UtilError(
    "Current partitions and generated partition replicas are out of sync current=%s, generated=%s , please regenerate"
    .format(current, generated))
  class ReplicationOutOfSync private[ReassignPartitionErrors](current: Int, generated: Int) extends UtilError(
    "Current replication factor and generated replication factor for replicas are out of sync current=%s, generated=%s , please regenerate"
      .format(current, generated))
  class NoValidAssignments private[ReassignPartitionErrors] extends UtilError("Cannot reassign partitions with no valid assignments!")
  class ReassignmentAlreadyInProgress private[ReassignPartitionErrors] extends UtilError("Partition reassignment currently in " +
    "progress for.  Aborting operation")
  class FailedToReassignPartitionReplicas private[ReassignPartitionErrors] (t: Throwable) extends UtilError(
    s"Failed to reassign partition replicas ${t.getStackTrace.mkString("[","\n","]")}")

  def MissingReplicaSetForPartition(part: Int) = new MissingReplicaSetForPartition(part)
  def ReassignmentDataEmptyForTopic(topic: String) = new ReassignmentDataEmptyForTopic(topic)
  def PartitionsOutOfSync(current: Int, generated: Int) = new PartitionsOutOfSync(current,generated)
  def ReplicationOutOfSync(current: Int, generated: Int) = new ReplicationOutOfSync(current,generated)
  val NoValidAssignments = new NoValidAssignments
  val AlreadyInProgress = new ReassignmentAlreadyInProgress
  def FailedToReassignPartitionReplicas(t: Throwable) = new FailedToReassignPartitionReplicas(t)
}
