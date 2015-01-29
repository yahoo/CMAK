/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import ActorModel.{BrokerList, TopicDescription}

import scala.util.Try

/**
 * @author hiral
 */
case class TopicPartitionIdentity(partNum: Int, leader:Int, isr: Set[Int], replicas: Set[Int], isPreferredLeader: Boolean = false, isUnderReplicated: Boolean = false)
object TopicPartitionIdentity {
  import play.api.libs.json._
  implicit def from(partition: Int, state:Option[String], replicas: Set[Int]) : TopicPartitionIdentity = {
    val leaderAndIsr = for {
      json <- state
      parsedJson = Json.parse(json)
    } yield ((parsedJson \ "leader").as[Int], (parsedJson \ "isr").as[Set[Int]])

    leaderAndIsr.fold(TopicPartitionIdentity(partition,-2,Set.empty,replicas)) { case (leader, isr) =>
      TopicPartitionIdentity(partition, leader, isr, replicas, leader == replicas.head, isr.size != replicas.size)
    }
  }
}

case class BrokerTopicPartitions(id: Int, partitions: IndexedSeq[Int], isSkewed: Boolean)

case class TopicIdentity(topic:String, partitions:Int, partitionsIdentity: Map[Int,TopicPartitionIdentity], numBrokers: Int) {

  val replicationFactor : Int = partitionsIdentity.head._2.replicas.size

  val partitionsByBroker : IndexedSeq[BrokerTopicPartitions] = {
    val brokerPartitionsMap : Map[Int, Iterable[Int]] =
      partitionsIdentity.toList.flatMap(t => t._2.isr.map(i => (i,t._2.partNum))).groupBy(_._1).mapValues(_.map(_._2))

    val brokersForTopic = brokerPartitionsMap.keySet.size
    val avgPartitionsPerBroker : Double = Math.ceil((1.0 * partitions) / brokersForTopic * replicationFactor)

    brokerPartitionsMap.map {
      case (brokerId, brokerPartitions)=>
        BrokerTopicPartitions(brokerId, brokerPartitions.toIndexedSeq,
          brokerPartitions.size > avgPartitionsPerBroker)
    }.toIndexedSeq.sortBy(_.id)
  }


  val preferredReplicasPercentage : Int = (100 * partitionsIdentity.count(_._2.isPreferredLeader)) / partitions

  val topicBrokers : Int = partitionsByBroker.size

  val brokersSkewPercentage : Int =  {
    if(topicBrokers > 0)
      (100 * partitionsByBroker.count(_.isSkewed)) / topicBrokers
    else 0
  }

  val brokersSpreadPercentage : Int = (100 * topicBrokers) / numBrokers
}

object TopicIdentity {

  implicit def from(brokers: Int,td: TopicDescription) : TopicIdentity = {
    import play.api.libs.json._
    val descJson = Json.parse(td.description)
    val partMap = (descJson \ "partitions").as[Map[String,Set[Int]]]
    val stateMap = td.partitionState.getOrElse(Map.empty)
    val tpi : Map[Int,TopicPartitionIdentity] = partMap.map { case (part, replicas) =>
      (part.toInt,TopicPartitionIdentity.from(part.toInt,stateMap.get(part),replicas))
      }.toMap
    TopicIdentity(td.topic,partMap.size,tpi,brokers)
  }

  implicit def from(bl: BrokerList,td: TopicDescription) : TopicIdentity = {
    from(bl.list.size, td)
  }

  implicit def reassignReplicas(currentTopicIdentity: TopicIdentity, assignedReplicas: Map[Int, Seq[Int]]) : Try[TopicIdentity] = {
    Try {
      val newTpi : Map[Int, TopicPartitionIdentity] = currentTopicIdentity.partitionsIdentity.map { case (part, tpi) =>
        val newReplicaSet = assignedReplicas.get(part)
        require(newReplicaSet.isDefined, s"Missing replica assignment for partition $part for topic ${currentTopicIdentity.topic}")
        (part,tpi.copy(replicas = newReplicaSet.get.toSet))
      }
      TopicIdentity(currentTopicIdentity.topic,currentTopicIdentity.partitions,newTpi,currentTopicIdentity.numBrokers)
    }
  }
}
