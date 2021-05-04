/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.manager.utils.one10

import kafka.api.{ApiVersion, KAFKA_2_1_IV0, KAFKA_2_1_IV1}
import kafka.common.OffsetAndMetadata
import kafka.coordinator.group.JoinGroupResult
import kafka.utils.{CoreUtils, Logging, nonthreadsafe}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.protocol.types.Type._
import org.apache.kafka.common.protocol.types._
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{KafkaException, MessageFormatter, TopicPartition}

import java.io.PrintStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Optional
import java.util.concurrent.locks.ReentrantLock
import scala.collection.JavaConverters._
import scala.collection.{Seq, immutable, mutable, _}

private[one10] sealed trait GroupState

/**
  * Group is preparing to rebalance
  *
  * action: respond to heartbeats with REBALANCE_IN_PROGRESS
  *         respond to sync group with REBALANCE_IN_PROGRESS
  *         remove member on leave group request
  *         park join group requests from new or existing members until all expected members have joined
  *         allow offset commits from previous generation
  *         allow offset fetch requests
  * transition: some members have joined by the timeout => CompletingRebalance
  *             all members have left the group => Empty
  *             group is removed by partition emigration => Dead
  */
private[one10] case object PreparingRebalance extends GroupState

/**
  * Group is awaiting state assignment from the leader
  *
  * action: respond to heartbeats with REBALANCE_IN_PROGRESS
  *         respond to offset commits with REBALANCE_IN_PROGRESS
  *         park sync group requests from followers until transition to Stable
  *         allow offset fetch requests
  * transition: sync group with state assignment received from leader => Stable
  *             join group from new member or existing member with updated metadata => PreparingRebalance
  *             leave group from existing member => PreparingRebalance
  *             member failure detected => PreparingRebalance
  *             group is removed by partition emigration => Dead
  */
private[one10] case object CompletingRebalance extends GroupState

/**
  * Group is stable
  *
  * action: respond to member heartbeats normally
  *         respond to sync group from any member with current assignment
  *         respond to join group from followers with matching metadata with current group metadata
  *         allow offset commits from member of current generation
  *         allow offset fetch requests
  * transition: member failure detected via heartbeat => PreparingRebalance
  *             leave group from existing member => PreparingRebalance
  *             leader join-group received => PreparingRebalance
  *             follower join-group with new metadata => PreparingRebalance
  *             group is removed by partition emigration => Dead
  */
private[one10] case object Stable extends GroupState

/**
  * Group has no more members and its metadata is being removed
  *
  * action: respond to join group with UNKNOWN_MEMBER_ID
  *         respond to sync group with UNKNOWN_MEMBER_ID
  *         respond to heartbeat with UNKNOWN_MEMBER_ID
  *         respond to leave group with UNKNOWN_MEMBER_ID
  *         respond to offset commit with UNKNOWN_MEMBER_ID
  *         allow offset fetch requests
  * transition: Dead is a final state before group metadata is cleaned up, so there are no transitions
  */
private[one10] case object Dead extends GroupState

/**
  * Group has no more members, but lingers until all offsets have expired. This state
  * also represents groups which use Kafka only for offset commits and have no members.
  *
  * action: respond normally to join group from new members
  *         respond to sync group with UNKNOWN_MEMBER_ID
  *         respond to heartbeat with UNKNOWN_MEMBER_ID
  *         respond to leave group with UNKNOWN_MEMBER_ID
  *         respond to offset commit with UNKNOWN_MEMBER_ID
  *         allow offset fetch requests
  * transition: last offsets removed in periodic expiration task => Dead
  *             join group from a new member => PreparingRebalance
  *             group is removed by partition emigration => Dead
  *             group is removed by expiration => Dead
  */
private[one10] case object Empty extends GroupState

/**
  * Case class used to represent group metadata for the ListGroups API
  */
case class GroupOverview(groupId: String,
                         protocolType: String)

/**
  * We cache offset commits along with their commit record offset. This enables us to ensure that the latest offset
  * commit is always materialized when we have a mix of transactional and regular offset commits. Without preserving
  * information of the commit record offset, compaction of the offsets topic it self may result in the wrong offset commit
  * being materialized.
  */
case class CommitRecordMetadataAndOffset(appendedBatchOffset: Option[Long], offsetAndMetadata: OffsetAndMetadata) {
  def olderThan(that: CommitRecordMetadataAndOffset) : Boolean = appendedBatchOffset.get < that.appendedBatchOffset.get
}

object GroupMetadata {
  private val validPreviousStates: Map[GroupState, Set[GroupState]] =
    Map(Dead -> Set(Stable, PreparingRebalance, CompletingRebalance, Empty, Dead),
      CompletingRebalance -> Set(PreparingRebalance),
      Stable -> Set(CompletingRebalance),
      PreparingRebalance -> Set(Stable, CompletingRebalance, Empty),
      Empty -> Set(PreparingRebalance))

  def loadGroup(groupId: String,
                initialState: GroupState,
                generationId: Int,
                protocolType: String,
                protocol: String,
                leaderId: String,
                currentStateTimestamp: Option[Long],
                members: Iterable[MemberMetadata],
                time: Time): GroupMetadata = {
    val group = new GroupMetadata(groupId, initialState, time)
    group.generationId = generationId
    group.protocolType = if (protocolType == null || protocolType.isEmpty) None else Some(protocolType)
    group.protocol = Option(protocol)
    group.leaderId = Option(leaderId)
    group.currentStateTimestamp = currentStateTimestamp
    members.foreach(group.add(_, null))
    group
  }
}

/**
  * Group contains the following metadata:
  *
  *  Membership metadata:
  *  1. Members registered in this group
  *  2. Current protocol assigned to the group (e.g. partition assignment strategy for consumers)
  *  3. Protocol metadata associated with group members
  *
  *  State metadata:
  *  1. group state
  *  2. generation id
  *  3. leader id
  */
@nonthreadsafe
class GroupMetadata(val groupId: String, initialState: GroupState, time: Time) extends Logging {
  type JoinCallback = JoinGroupResult => Unit

  private[one10] val lock = new ReentrantLock

  private var state: GroupState = initialState
  var currentStateTimestamp: Option[Long] = Some(time.milliseconds())
  var protocolType: Option[String] = None
  var generationId = 0
  private var leaderId: Option[String] = None
  private var protocol: Option[String] = None

  private val members = new mutable.HashMap[String, MemberMetadata]
  private val pendingMembers = new mutable.HashSet[String]
  private var numMembersAwaitingJoin = 0
  private val supportedProtocols = new mutable.HashMap[String, Integer]().withDefaultValue(0)
  private val offsets = new mutable.HashMap[TopicPartition, CommitRecordMetadataAndOffset]
  private val pendingOffsetCommits = new mutable.HashMap[TopicPartition, OffsetAndMetadata]
  private val pendingTransactionalOffsetCommits = new mutable.HashMap[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]()
  private var receivedTransactionalOffsetCommits = false
  private var receivedConsumerOffsetCommits = false

  var newMemberAdded: Boolean = false

  def inLock[T](fun: => T): T = CoreUtils.inLock(lock)(fun)

  def is(groupState: GroupState) = state == groupState
  def not(groupState: GroupState) = state != groupState
  def has(memberId: String) = members.contains(memberId)
  def get(memberId: String) = members(memberId)
  def size = members.size

  def isLeader(memberId: String): Boolean = leaderId.contains(memberId)
  def leaderOrNull: String = leaderId.orNull
  def protocolOrNull: String = protocol.orNull
  def currentStateTimestampOrDefault: Long = currentStateTimestamp.getOrElse(-1)

  def add(member: MemberMetadata, callback: JoinCallback = null) {
    if (members.isEmpty)
      this.protocolType = Some(member.protocolType)

    assert(groupId == member.groupId)
    assert(this.protocolType.orNull == member.protocolType)
    //assert(supportsProtocols(member.protocolType, member.supportedProtocols))

    if (leaderId.isEmpty)
      leaderId = Some(member.memberId)
    members.put(member.memberId, member)
    member.supportedProtocols.foreach{ case (protocolType, protocolSet) => protocolSet.foreach { protocol => supportedProtocols(protocol) += 1} }
  }

  def allMembers = members.keySet

  def allMemberMetadata = members.values.toList

  def currentState = state

  /* Remove a pending transactional offset commit if the actual offset commit record was not written to the log.
   * We will return an error and the client will retry the request, potentially to a different coordinator.
   */
  def failPendingTxnOffsetCommit(producerId: Long, topicPartition: TopicPartition): Unit = {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffsets) =>
        val pendingOffsetCommit = pendingOffsets.remove(topicPartition)
        trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetCommit failed " +
          s"to be appended to the log")
        if (pendingOffsets.isEmpty)
          pendingTransactionalOffsetCommits.remove(producerId)
      case _ =>
      // We may hit this case if the partition in question has emigrated already.
    }
  }

  def onTxnOffsetCommitAppend(producerId: Long, topicPartition: TopicPartition,
                              commitRecordMetadataAndOffset: CommitRecordMetadataAndOffset) {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffset) =>
        if (pendingOffset.contains(topicPartition)
          && pendingOffset(topicPartition).offsetAndMetadata == commitRecordMetadataAndOffset.offsetAndMetadata)
          pendingOffset.update(topicPartition, commitRecordMetadataAndOffset)
      case _ =>
      // We may hit this case if the partition in question has emigrated.
    }
  }

  /* Complete a pending transactional offset commit. This is called after a commit or abort marker is fully written
   * to the log.
   */
  def completePendingTxnOffsetCommit(producerId: Long, isCommit: Boolean): Unit = {
    val pendingOffsetsOpt = pendingTransactionalOffsetCommits.remove(producerId)
    if (isCommit) {
      pendingOffsetsOpt.foreach { pendingOffsets =>
        pendingOffsets.foreach { case (topicPartition, commitRecordMetadataAndOffset) =>
          if (commitRecordMetadataAndOffset.appendedBatchOffset.isEmpty)
            throw new IllegalStateException(s"Trying to complete a transactional offset commit for producerId $producerId " +
              s"and groupId $groupId even though the offset commit record itself hasn't been appended to the log.")

          val currentOffsetOpt = offsets.get(topicPartition)
          if (currentOffsetOpt.forall(_.olderThan(commitRecordMetadataAndOffset))) {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              "committed and loaded into the cache.")
            offsets.put(topicPartition, commitRecordMetadataAndOffset)
          } else {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              s"committed, but not loaded since its offset is older than current offset $currentOffsetOpt.")
          }
        }
      }
    } else {
      trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetsOpt aborted")
    }
  }

  def activeProducers = pendingTransactionalOffsetCommits.keySet

  def hasPendingOffsetCommitsFromProducer(producerId: Long) =
    pendingTransactionalOffsetCommits.contains(producerId)

  def removeAllOffsets(): immutable.Map[TopicPartition, OffsetAndMetadata] = removeOffsets(offsets.keySet.toSeq)

  def removeOffsets(topicPartitions: Seq[TopicPartition]): immutable.Map[TopicPartition, OffsetAndMetadata] = {
    topicPartitions.flatMap { topicPartition =>
      pendingOffsetCommits.remove(topicPartition)
      pendingTransactionalOffsetCommits.foreach { case (_, pendingOffsets) =>
        pendingOffsets.remove(topicPartition)
      }
      val removedOffset = offsets.remove(topicPartition)
      removedOffset.map(topicPartition -> _.offsetAndMetadata)
    }.toMap
  }

  def removeExpiredOffsets(currentTimestamp: Long, offsetRetentionMs: Long) : Map[TopicPartition, OffsetAndMetadata] = {

    def getExpiredOffsets(baseTimestamp: CommitRecordMetadataAndOffset => Long): Map[TopicPartition, OffsetAndMetadata] = {
      offsets.filter {
        case (topicPartition, commitRecordMetadataAndOffset) =>
          !pendingOffsetCommits.contains(topicPartition) && {
            commitRecordMetadataAndOffset.offsetAndMetadata.expireTimestamp match {
              case None =>
                // current version with no per partition retention
                currentTimestamp - baseTimestamp(commitRecordMetadataAndOffset) >= offsetRetentionMs
              case Some(expireTimestamp) =>
                // older versions with explicit expire_timestamp field => old expiration semantics is used
                currentTimestamp >= expireTimestamp
            }
          }
      }.map {
        case (topicPartition, commitRecordOffsetAndMetadata) =>
          (topicPartition, commitRecordOffsetAndMetadata.offsetAndMetadata)
      }.toMap
    }

    val expiredOffsets: Map[TopicPartition, OffsetAndMetadata] = protocolType match {
      case Some(_) if is(Empty) =>
        // no consumer exists in the group =>
        // - if current state timestamp exists and retention period has passed since group became Empty,
        //   expire all offsets with no pending offset commit;
        // - if there is no current state timestamp (old group metadata schema) and retention period has passed
        //   since the last commit timestamp, expire the offset
        getExpiredOffsets(commitRecordMetadataAndOffset =>
          currentStateTimestamp.getOrElse(commitRecordMetadataAndOffset.offsetAndMetadata.commitTimestamp))

      case None =>
        // protocolType is None => standalone (simple) consumer, that uses Kafka for offset storage only
        // expire offsets with no pending offset commit that retention period has passed since their last commit
        getExpiredOffsets(_.offsetAndMetadata.commitTimestamp)

      case _ =>
        Map()
    }

    if (expiredOffsets.nonEmpty)
      debug(s"Expired offsets from group '$groupId': ${expiredOffsets.keySet}")

    offsets --= expiredOffsets.keySet
    expiredOffsets
  }

  def allOffsets = offsets.map { case (topicPartition, commitRecordMetadataAndOffset) =>
    (topicPartition, commitRecordMetadataAndOffset.offsetAndMetadata)
  }.toMap

  def offset(topicPartition: TopicPartition): Option[OffsetAndMetadata] = offsets.get(topicPartition).map(_.offsetAndMetadata)

  // visible for testing
  private[one10] def offsetWithRecordMetadata(topicPartition: TopicPartition): Option[CommitRecordMetadataAndOffset] = offsets.get(topicPartition)

  def numOffsets = offsets.size

  def hasOffsets = offsets.nonEmpty || pendingOffsetCommits.nonEmpty || pendingTransactionalOffsetCommits.nonEmpty

  /*
  private def assertValidTransition(targetState: GroupState) {
    if (!GroupMetadata.validPreviousStates(targetState).contains(state))
      throw new IllegalStateException("Group %s should be in the %s states before moving to %s state. Instead it is in %s state"
        .format(groupId, GroupMetadata.validPreviousStates(targetState).mkString(","), targetState, state))
  }
  */

  override def toString: String = {
    "GroupMetadata(" +
      s"groupId=$groupId, " +
      s"generation=$generationId, " +
      s"protocolType=$protocolType, " +
      s"currentState=$currentState, " +
      s"members=$members)"
  }

}

/**
  * Messages stored for the group topic has versions for both the key and value fields. Key
  * version is used to indicate the type of the message (also to differentiate different types
  * of messages from being compacted together if they have the same field values); and value
  * version is used to evolve the messages within their data types:
  *
  * key version 0:       group consumption offset
  *    -> value version 0:       [offset, metadata, timestamp]
  *
  * key version 1:       group consumption offset
  *    -> value version 1:       [offset, metadata, commit_timestamp, expire_timestamp]
  *
  * key version 2:       group metadata
  *     -> value version 0:       [protocol_type, generation, protocol, leader, members]
  */
object GroupMetadataManager {

  private val CURRENT_OFFSET_KEY_SCHEMA_VERSION = 1.toShort
  private val CURRENT_GROUP_KEY_SCHEMA_VERSION = 2.toShort

  private val OFFSET_COMMIT_KEY_SCHEMA = new Schema(new Field("group", STRING),
    new Field("topic", STRING),
    new Field("partition", INT32))
  private val OFFSET_KEY_GROUP_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("group")
  private val OFFSET_KEY_TOPIC_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("topic")
  private val OFFSET_KEY_PARTITION_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("partition")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(new Field("offset", INT64),
    new Field("metadata", STRING, "Associated metadata.", ""),
    new Field("timestamp", INT64))
  private val OFFSET_VALUE_OFFSET_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("offset")
  private val OFFSET_VALUE_METADATA_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("metadata")
  private val OFFSET_VALUE_TIMESTAMP_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("timestamp")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V1 = new Schema(new Field("offset", INT64),
    new Field("metadata", STRING, "Associated metadata.", ""),
    new Field("commit_timestamp", INT64),
    new Field("expire_timestamp", INT64))
  private val OFFSET_VALUE_OFFSET_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("offset")
  private val OFFSET_VALUE_METADATA_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("metadata")
  private val OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("commit_timestamp")
  private val OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("expire_timestamp")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V2 = new Schema(new Field("offset", INT64),
    new Field("metadata", STRING, "Associated metadata.", ""),
    new Field("commit_timestamp", INT64))
  private val OFFSET_VALUE_OFFSET_FIELD_V2 = OFFSET_COMMIT_VALUE_SCHEMA_V2.get("offset")
  private val OFFSET_VALUE_METADATA_FIELD_V2 = OFFSET_COMMIT_VALUE_SCHEMA_V2.get("metadata")
  private val OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V2 = OFFSET_COMMIT_VALUE_SCHEMA_V2.get("commit_timestamp")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V3 = new Schema(
    new Field("offset", INT64),
    new Field("leader_epoch", INT32),
    new Field("metadata", STRING, "Associated metadata.", ""),
    new Field("commit_timestamp", INT64))
  private val OFFSET_VALUE_OFFSET_FIELD_V3 = OFFSET_COMMIT_VALUE_SCHEMA_V3.get("offset")
  private val OFFSET_VALUE_LEADER_EPOCH_FIELD_V3 = OFFSET_COMMIT_VALUE_SCHEMA_V3.get("leader_epoch")
  private val OFFSET_VALUE_METADATA_FIELD_V3 = OFFSET_COMMIT_VALUE_SCHEMA_V3.get("metadata")
  private val OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V3 = OFFSET_COMMIT_VALUE_SCHEMA_V3.get("commit_timestamp")

  private val GROUP_METADATA_KEY_SCHEMA = new Schema(new Field("group", STRING))
  private val GROUP_KEY_GROUP_FIELD = GROUP_METADATA_KEY_SCHEMA.get("group")

  private val MEMBER_ID_KEY = "member_id"
  private val CLIENT_ID_KEY = "client_id"
  private val CLIENT_HOST_KEY = "client_host"
  private val REBALANCE_TIMEOUT_KEY = "rebalance_timeout"
  private val SESSION_TIMEOUT_KEY = "session_timeout"
  private val SUBSCRIPTION_KEY = "subscription"
  private val ASSIGNMENT_KEY = "assignment"

  private val MEMBER_METADATA_V0 = new Schema(
    new Field(MEMBER_ID_KEY, STRING),
    new Field(CLIENT_ID_KEY, STRING),
    new Field(CLIENT_HOST_KEY, STRING),
    new Field(SESSION_TIMEOUT_KEY, INT32),
    new Field(SUBSCRIPTION_KEY, BYTES),
    new Field(ASSIGNMENT_KEY, BYTES))

  private val MEMBER_METADATA_V1 = new Schema(
    new Field(MEMBER_ID_KEY, STRING),
    new Field(CLIENT_ID_KEY, STRING),
    new Field(CLIENT_HOST_KEY, STRING),
    new Field(REBALANCE_TIMEOUT_KEY, INT32),
    new Field(SESSION_TIMEOUT_KEY, INT32),
    new Field(SUBSCRIPTION_KEY, BYTES),
    new Field(ASSIGNMENT_KEY, BYTES))

  private val MEMBER_METADATA_V2 = MEMBER_METADATA_V1

  private val PROTOCOL_TYPE_KEY = "protocol_type"
  private val GENERATION_KEY = "generation"
  private val PROTOCOL_KEY = "protocol"
  private val LEADER_KEY = "leader"
  private val CURRENT_STATE_TIMESTAMP_KEY = "current_state_timestamp"
  private val MEMBERS_KEY = "members"

  private val GROUP_METADATA_VALUE_SCHEMA_V0 = new Schema(
    new Field(PROTOCOL_TYPE_KEY, STRING),
    new Field(GENERATION_KEY, INT32),
    new Field(PROTOCOL_KEY, NULLABLE_STRING),
    new Field(LEADER_KEY, NULLABLE_STRING),
    new Field(MEMBERS_KEY, new ArrayOf(MEMBER_METADATA_V0)))

  private val GROUP_METADATA_VALUE_SCHEMA_V1 = new Schema(
    new Field(PROTOCOL_TYPE_KEY, STRING),
    new Field(GENERATION_KEY, INT32),
    new Field(PROTOCOL_KEY, NULLABLE_STRING),
    new Field(LEADER_KEY, NULLABLE_STRING),
    new Field(MEMBERS_KEY, new ArrayOf(MEMBER_METADATA_V1)))

  private val GROUP_METADATA_VALUE_SCHEMA_V2 = new Schema(
    new Field(PROTOCOL_TYPE_KEY, STRING),
    new Field(GENERATION_KEY, INT32),
    new Field(PROTOCOL_KEY, NULLABLE_STRING),
    new Field(LEADER_KEY, NULLABLE_STRING),
    new Field(CURRENT_STATE_TIMESTAMP_KEY, INT64),
    new Field(MEMBERS_KEY, new ArrayOf(MEMBER_METADATA_V2)))

  // map of versions to key schemas as data types
  private val MESSAGE_TYPE_SCHEMAS = Map(
    0 -> OFFSET_COMMIT_KEY_SCHEMA,
    1 -> OFFSET_COMMIT_KEY_SCHEMA,
    2 -> GROUP_METADATA_KEY_SCHEMA)

  // map of version of offset value schemas
  private val OFFSET_VALUE_SCHEMAS = Map(
    0 -> OFFSET_COMMIT_VALUE_SCHEMA_V0,
    1 -> OFFSET_COMMIT_VALUE_SCHEMA_V1,
    2 -> OFFSET_COMMIT_VALUE_SCHEMA_V2,
    3 -> OFFSET_COMMIT_VALUE_SCHEMA_V3)

  // map of version of group metadata value schemas
  private val GROUP_VALUE_SCHEMAS = Map(
    0 -> GROUP_METADATA_VALUE_SCHEMA_V0,
    1 -> GROUP_METADATA_VALUE_SCHEMA_V1,
    2 -> GROUP_METADATA_VALUE_SCHEMA_V2)

  private val CURRENT_OFFSET_KEY_SCHEMA = schemaForKey(CURRENT_OFFSET_KEY_SCHEMA_VERSION)
  private val CURRENT_GROUP_KEY_SCHEMA = schemaForKey(CURRENT_GROUP_KEY_SCHEMA_VERSION)

  //private val CURRENT_OFFSET_VALUE_SCHEMA_VERSION = 1.toShort
  //private val CURRENT_GROUP_VALUE_SCHEMA_VERSION = 1.toShort

  //private val CURRENT_OFFSET_VALUE_SCHEMA = schemaForOffset(CURRENT_OFFSET_VALUE_SCHEMA_VERSION)
  //private val CURRENT_GROUP_VALUE_SCHEMA = schemaForGroup(CURRENT_GROUP_VALUE_SCHEMA_VERSION)

  private def schemaForKey(version: Int) = {
    val schemaOpt = MESSAGE_TYPE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown offset schema version " + version)
    }
  }

  private def schemaForOffsetValue(version: Int) = {
    val schemaOpt = OFFSET_VALUE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown offset schema version " + version)
    }
  }

  private def schemaForGroupValue(version: Int) = {
    val schemaOpt = GROUP_VALUE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown group metadata version " + version)
    }
  }

  /**
    * Generates the key for offset commit message for given (group, topic, partition)
    *
    * @return key for offset commit message
    */
  private[one10] def offsetCommitKey(group: String,
                                     topicPartition: TopicPartition): Array[Byte] = {
    val key = new Struct(CURRENT_OFFSET_KEY_SCHEMA)
    key.set(OFFSET_KEY_GROUP_FIELD, group)
    key.set(OFFSET_KEY_TOPIC_FIELD, topicPartition.topic)
    key.set(OFFSET_KEY_PARTITION_FIELD, topicPartition.partition)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_OFFSET_KEY_SCHEMA_VERSION)
    key.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
    * Generates the key for group metadata message for given group
    *
    * @return key bytes for group metadata message
    */
  private[one10] def groupMetadataKey(group: String): Array[Byte] = {
    val key = new Struct(CURRENT_GROUP_KEY_SCHEMA)
    key.set(GROUP_KEY_GROUP_FIELD, group)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_GROUP_KEY_SCHEMA_VERSION)
    key.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
    * Generates the payload for offset commit message from given offset and metadata
    *
    * @param offsetAndMetadata consumer's current offset and metadata
    * @param apiVersion the api version
    * @return payload for offset commit message
    */
  private[one10] def offsetCommitValue(offsetAndMetadata: OffsetAndMetadata,
                                       apiVersion: ApiVersion): Array[Byte] = {
    // generate commit value according to schema version
    val (version, value) = {
      if (apiVersion < KAFKA_2_1_IV0 || offsetAndMetadata.expireTimestamp.nonEmpty) {
        val value = new Struct(OFFSET_COMMIT_VALUE_SCHEMA_V1)
        value.set(OFFSET_VALUE_OFFSET_FIELD_V1, offsetAndMetadata.offset)
        value.set(OFFSET_VALUE_METADATA_FIELD_V1, offsetAndMetadata.metadata)
        value.set(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1, offsetAndMetadata.commitTimestamp)
        // version 1 has a non empty expireTimestamp field
        value.set(OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1,
          offsetAndMetadata.expireTimestamp.getOrElse(-1L))
        (1, value)
      } else if (apiVersion < KAFKA_2_1_IV1) {
        val value = new Struct(OFFSET_COMMIT_VALUE_SCHEMA_V2)
        value.set(OFFSET_VALUE_OFFSET_FIELD_V2, offsetAndMetadata.offset)
        value.set(OFFSET_VALUE_METADATA_FIELD_V2, offsetAndMetadata.metadata)
        value.set(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V2, offsetAndMetadata.commitTimestamp)
        (2, value)
      } else {
        val value = new Struct(OFFSET_COMMIT_VALUE_SCHEMA_V3)
        value.set(OFFSET_VALUE_OFFSET_FIELD_V3, offsetAndMetadata.offset)
        value.set(OFFSET_VALUE_LEADER_EPOCH_FIELD_V3,
          offsetAndMetadata.leaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
        value.set(OFFSET_VALUE_METADATA_FIELD_V3, offsetAndMetadata.metadata)
        value.set(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V3, offsetAndMetadata.commitTimestamp)
        (3, value)
      }
    }

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(version.toShort)
    value.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
    * Decodes the offset messages' key
    *
    * @param buffer input byte-buffer
    * @return an GroupTopicPartition object
    */
  def readMessageKey(buffer: ByteBuffer): BaseKey = {
    val version = buffer.getShort
    val keySchema = schemaForKey(version)
    val key = keySchema.read(buffer)

    if (version <= CURRENT_OFFSET_KEY_SCHEMA_VERSION) {
      // version 0 and 1 refer to offset
      val group = key.get(OFFSET_KEY_GROUP_FIELD).asInstanceOf[String]
      val topic = key.get(OFFSET_KEY_TOPIC_FIELD).asInstanceOf[String]
      val partition = key.get(OFFSET_KEY_PARTITION_FIELD).asInstanceOf[Int]

      OffsetKey(version, GroupTopicPartition(group, new TopicPartition(topic, partition)))

    } else if (version == CURRENT_GROUP_KEY_SCHEMA_VERSION) {
      // version 2 refers to offset
      val group = key.get(GROUP_KEY_GROUP_FIELD).asInstanceOf[String]

      GroupMetadataKey(version, group)
    } else {
      throw new IllegalStateException(s"Unknown group metadata message version: $version")
    }
  }

  /**
    * Decodes the offset messages' payload and retrieves offset and metadata from it
    *
    * @param buffer input byte-buffer
    * @return an offset-metadata object from the message
    */
  def readOffsetMessageValue(buffer: ByteBuffer): OffsetAndMetadata = {
    if (buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForOffsetValue(version)
      val value = valueSchema.read(buffer)

      if (version == 0) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V0).asInstanceOf[Long]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V0).asInstanceOf[String]
        val timestamp = value.get(OFFSET_VALUE_TIMESTAMP_FIELD_V0).asInstanceOf[Long]

        OffsetAndMetadata(offset, metadata, timestamp)
      } else if (version == 1) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V1).asInstanceOf[Long]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V1).asInstanceOf[String]
        val commitTimestamp = value.get(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1).asInstanceOf[Long]
        val expireTimestamp = value.get(OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1).asInstanceOf[Long]

        if (expireTimestamp == -1L)
          OffsetAndMetadata(offset, metadata, commitTimestamp)
        else
          OffsetAndMetadata(offset, metadata, commitTimestamp, expireTimestamp)
      } else if (version == 2) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V2).asInstanceOf[Long]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V2).asInstanceOf[String]
        val commitTimestamp = value.get(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V2).asInstanceOf[Long]

        OffsetAndMetadata(offset, metadata, commitTimestamp)
      } else if (version == 3) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V3).asInstanceOf[Long]
        val leaderEpoch = value.get(OFFSET_VALUE_LEADER_EPOCH_FIELD_V3).asInstanceOf[Int]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V3).asInstanceOf[String]
        val commitTimestamp = value.get(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V3).asInstanceOf[Long]

        val leaderEpochOpt: Optional[Integer] = if (leaderEpoch < 0) Optional.empty() else Optional.of(leaderEpoch)
        OffsetAndMetadata(offset, leaderEpochOpt, metadata, commitTimestamp, null)
      } else {
        throw new IllegalStateException(s"Unknown offset message version: $version")
      }
    }
  }


  /**
    * Decodes the group metadata messages' payload and retrieves its member metadata from it
    *
    * @param buffer input byte-buffer
    * @param time the time instance to use
    * @return a group metadata object from the message
    */
  def readGroupMessageValue(groupId: String, buffer: ByteBuffer, time: Time): GroupMetadata = {
    if (buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForGroupValue(version)
      val value = valueSchema.read(buffer)

      if (version >= 0 && version <= 2) {
        val generationId = value.get(GENERATION_KEY).asInstanceOf[Int]
        val protocolType = value.get(PROTOCOL_TYPE_KEY).asInstanceOf[String]
        val protocol = value.get(PROTOCOL_KEY).asInstanceOf[String]
        val leaderId = value.get(LEADER_KEY).asInstanceOf[String]
        val memberMetadataArray = value.getArray(MEMBERS_KEY)
        val initialState = if (memberMetadataArray.isEmpty) Empty else Stable
        val currentStateTimestamp: Option[Long] = version match {
          case version if version == 2 =>
            if (value.hasField(CURRENT_STATE_TIMESTAMP_KEY)) {
              val timestamp = value.getLong(CURRENT_STATE_TIMESTAMP_KEY)
              if (timestamp == -1) None else Some(timestamp)
            } else
              None
          case _ =>
            None
        }

        val members = memberMetadataArray.map { memberMetadataObj =>
          val memberMetadata = memberMetadataObj.asInstanceOf[Struct]
          val memberId = memberMetadata.get(MEMBER_ID_KEY).asInstanceOf[String]
          val clientId = memberMetadata.get(CLIENT_ID_KEY).asInstanceOf[String]
          val clientHost = memberMetadata.get(CLIENT_HOST_KEY).asInstanceOf[String]
          val subscription = ConsumerProtocol.deserializeSubscription(memberMetadata.get(SUBSCRIPTION_KEY).asInstanceOf[ByteBuffer])
          val assignment = ConsumerProtocol.deserializeAssignment(memberMetadata.get(ASSIGNMENT_KEY).asInstanceOf[ByteBuffer])
          val member = new MemberMetadata(memberId
            , groupId
            , clientId
            , clientHost
            , protocolType
            , List((protocol, subscription.topics().asScala.toSet))
            , assignment.partitions().asScala.map(tp => (tp.topic(), tp.partition())).toSet)
          member
        }
        GroupMetadata.loadGroup(groupId, initialState, generationId, protocolType, protocol, leaderId, currentStateTimestamp, members, time)
      } else {
        throw new IllegalStateException(s"Unknown group metadata message version: $version")
      }
    }
  }


  // Formatter for use with tools such as console consumer: Consumer should also set exclude.internal.topics to false.
  // (specify --formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter" when consuming __consumer_offsets)
  class OffsetsMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream) {
      Option(consumerRecord.key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
        // Only print if the message is an offset record.
        // We ignore the timestamp of the message because GroupMetadataMessage has its own timestamp.
        case offsetKey: OffsetKey =>
          val groupTopicPartition = offsetKey.key
          val value = consumerRecord.value
          val formattedValue =
            if (value == null) "NULL"
            else GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(value)).toString
          output.write(groupTopicPartition.toString.getBytes(StandardCharsets.UTF_8))
          output.write("::".getBytes(StandardCharsets.UTF_8))
          output.write(formattedValue.getBytes(StandardCharsets.UTF_8))
          output.write("\n".getBytes(StandardCharsets.UTF_8))
        case _ => // no-op
      }
    }
  }

  // Formatter for use with tools to read group metadata history
  class GroupMetadataMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream) {
      Option(consumerRecord.key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
        // Only print if the message is a group metadata record.
        // We ignore the timestamp of the message because GroupMetadataMessage has its own timestamp.
        case groupMetadataKey: GroupMetadataKey =>
          val groupId = groupMetadataKey.key
          val value = consumerRecord.value
          val formattedValue =
            if (value == null) "NULL"
            else GroupMetadataManager.readGroupMessageValue(groupId, ByteBuffer.wrap(value), Time.SYSTEM).toString
          output.write(groupId.getBytes(StandardCharsets.UTF_8))
          output.write("::".getBytes(StandardCharsets.UTF_8))
          output.write(formattedValue.getBytes(StandardCharsets.UTF_8))
          output.write("\n".getBytes(StandardCharsets.UTF_8))
        case _ => // no-op
      }
    }
  }

}

case class GroupTopicPartition(group: String, topicPartition: TopicPartition) {

  def this(group: String, topic: String, partition: Int) =
    this(group, new TopicPartition(topic, partition))

  override def toString: String =
    "[%s,%s,%d]".format(group, topicPartition.topic, topicPartition.partition)
}

trait BaseKey{
  def version: Short
  def key: Any
}

case class OffsetKey(version: Short, key: GroupTopicPartition) extends BaseKey {

  override def toString: String = key.toString
}

case class GroupMetadataKey(version: Short, key: String) extends BaseKey {

  override def toString: String = key
}


