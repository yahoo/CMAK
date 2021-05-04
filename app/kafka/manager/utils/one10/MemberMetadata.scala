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

package kafka.manager.utils.one10
import org.apache.kafka.clients.admin.{ConsumerGroupDescription, MemberDescription}

object MemberMetadata {
  import collection.JavaConverters._
  def from(groupId: String, groupSummary: ConsumerGroupDescription, memberSummary: MemberDescription) : MemberMetadata = {
    val assignment = memberSummary.assignment().topicPartitions().asScala.map(tp => tp.topic() -> tp.partition()).toSet
    MemberMetadata(
      memberSummary.consumerId()
      , groupId
      , memberSummary.clientId
      , memberSummary.host()
      , "(n/a on backfill)"
      , List.empty
      , assignment
    )
  }
}

/**
  * Member metadata contains the following metadata:
  *
  * Heartbeat metadata:
  * 1. negotiated heartbeat session timeout
  * 2. timestamp of the latest heartbeat
  *
  * Protocol metadata:
  * 1. the list of supported protocols (ordered by preference)
  * 2. the metadata associated with each protocol
  *
  * In addition, it also contains the following state information:
  *
  * 1. Awaiting rebalance callback: when the group is in the prepare-rebalance state,
  *                                 its rebalance callback will be kept in the metadata if the
  *                                 member has sent the join group request
  * 2. Awaiting sync callback: when the group is in the awaiting-sync state, its sync callback
  *                            is kept in metadata until the leader provides the group assignment
  *                            and the group transitions to stable
  */

case class MemberMetadata(memberId: String,
                          groupId: String,
                          clientId: String,
                          clientHost: String,
                          protocolType: String,
                          supportedProtocols: List[(String, Set[String])],
                          assignment: Set[(String, Int)]
                    ) {

  def protocols = supportedProtocols.map(_._1).toSet

  def metadata(protocol: String): Set[String] = {
    supportedProtocols.find(_._1 == protocol) match {
      case Some((_, metadata)) => metadata
      case None =>
        throw new IllegalArgumentException("Member does not support protocol")
    }
  }

  override def toString: String = {
    "MemberMetadata(" +
      s"memberId=$memberId, " +
      s"clientId=$clientId, " +
      s"clientHost=$clientHost, " +
      s"supportedProtocols=${supportedProtocols.map(_._1)}, " +
      ")"
  }

}

