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


import scala.util.matching.Regex

/**
 * Borrowed from kafka 0.8.1.1
 * https://git-wip-us.apache.org/repos/asf?p=kafka.git;a=blob;f=core/src/main/scala/kafka/common/Topic.scala
 */
object Topic {
  import kafka.manager.utils.TopicErrors._

  val legalChars = "[a-zA-Z0-9\\._\\-]"
  val maxNameLength = 255
  private val rgx = new Regex(legalChars + "+")

  def validate(topic: String) {
    checkCondition(topic.length > 0, TopicNameEmpty)
    checkCondition(!(topic.equals(".") || topic.equals("..")), InvalidTopicName)
    checkCondition(topic.length <= maxNameLength, InvalidTopicLength)
    rgx.findFirstIn(topic) match {
      case Some(t) =>
        checkCondition(t.equals(topic), IllegalCharacterInName(topic))
      case None =>
        checkCondition(false, IllegalCharacterInName(topic))
    }
  }
}

object TopicErrors {
  class TopicNameEmpty private[TopicErrors] extends UtilError("topic name is illegal, can't be empty")
  class InvalidTopicName private[TopicErrors] extends UtilError("topic name cannot be \".\" or \"..\"")
  class InvalidTopicLength private[TopicErrors] extends UtilError(
    "topic name is illegal, can't be longer than " + Topic.maxNameLength + " characters")
  class IllegalCharacterInName private[TopicErrors] (topic: String) extends UtilError(
    "topic name " + topic + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'")
  class PartitionsGreaterThanZero private[TopicErrors] extends UtilError(s"number of partitions must be greater than 0!")
  class ReplicationGreaterThanZero private[TopicErrors] extends UtilError(s"replication factor must be greater than 0!")
  class ReplicationGreaterThanNumBrokers private[TopicErrors](replicationFactor: Int, numBrokers: Int) extends UtilError(
    s"replication factor: $replicationFactor larger than available brokers $numBrokers")
  class InconsistentPartitionReplicas private[TopicErrors] extends UtilError("All partitions should have the same number of replicas.")
  class TopicAlreadyExists private[TopicErrors] (topic: String) extends UtilError(s"Topic already exists : $topic")
  class DuplicateReplicaAssignment private[TopicErrors] (topic: String, part: Int, replicas: Seq[Int]) extends UtilError(
    s"Duplicate replica assignment topic=$topic, partition=$part, replicas=$replicas"
  )

  val TopicNameEmpty = new TopicNameEmpty
  val InvalidTopicName = new InvalidTopicName
  val InvalidTopicLength = new InvalidTopicLength
  def IllegalCharacterInName(topic: String) = new IllegalCharacterInName(topic)
  val PartitionsGreaterThanZero = new PartitionsGreaterThanZero
  val ReplicationGreaterThanZero = new ReplicationGreaterThanZero
  def ReplicationGreaterThanNumBrokers(rf: Int, nb: Int) = new ReplicationGreaterThanNumBrokers(rf,nb)
  val InconsistentPartitionReplicas = new InconsistentPartitionReplicas
  def TopicAlreadyExists(topic: String) = new TopicAlreadyExists(topic)
  def DuplicateReplicAssignment(topic: String, part: Int, replicas: Seq[Int]) = new DuplicateReplicaAssignment(topic,part,replicas)
}

