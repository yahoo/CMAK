/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import ActorModel.{BrokerList, TopicDescription}
import org.slf4j.LoggerFactory

import scala.util.Try

/**
 * @author hiral
 */
case class TopicPartitionIdentity(partNum: Int, leader:Int, isr: Seq[Int], replicas: Seq[Int], isPreferredLeader: Boolean = false, isUnderReplicated: Boolean = false)
object TopicPartitionIdentity {
  import play.api.libs.json._
  implicit def from(partition: Int, state:Option[String], replicas: Seq[Int]) : TopicPartitionIdentity = {
    val leaderAndIsr = for {
      json <- state
      parsedJson = Json.parse(json)
    } yield ((parsedJson \ "leader").as[Int], (parsedJson \ "isr").as[Seq[Int]])

    leaderAndIsr.fold(TopicPartitionIdentity(partition,-2,Seq.empty,replicas)) { case (leader, isr) =>
      TopicPartitionIdentity(partition, leader, isr, replicas, leader == replicas.head, isr.size != replicas.size)
    }
  }
}

case class BrokerTopicPartitions(id: Int, partitions: IndexedSeq[Int], isSkewed: Boolean)

case class TopicIdentity(topic:String, 
                         readVersion: Int,
                         partitions:Int, 
                         partitionsIdentity: Map[Int,TopicPartitionIdentity], 
                         numBrokers: Int, 
                         configReadVersion: Int, 
                         config: List[(String,String)], deleteSupported: Boolean) {

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
  
  val underReplicatedPercentage : Int = (100 * partitionsIdentity.count(_._2.isUnderReplicated)) / partitions

  val topicBrokers : Int = partitionsByBroker.size

  val brokersSkewPercentage : Int =  {
    if(topicBrokers > 0)
      (100 * partitionsByBroker.count(_.isSkewed)) / topicBrokers
    else 0
  }

  val brokersSpreadPercentage : Int = if(numBrokers > 0) {
    (100 * topicBrokers) / numBrokers
  } else {
    100 // everthing is spreaded if nothing has to be spreaded
  }
  
}

object TopicIdentity {
  
  lazy val logger = LoggerFactory.getLogger(this.getClass)

  implicit def from(brokers: Int,td: TopicDescription) : TopicIdentity = {
    import play.api.libs.json._
    val descJson = Json.parse(td.description._2)
    val partMap = (descJson \ "partitions").as[Map[String,Seq[Int]]]
    val stateMap = td.partitionState.getOrElse(Map.empty)
    val tpi : Map[Int,TopicPartitionIdentity] = partMap.map { case (part, replicas) =>
      (part.toInt,TopicPartitionIdentity.from(part.toInt,stateMap.get(part),replicas))
      }.toMap
    val config : (Int,Map[String, String]) = {
      try {
        val resultOption: Option[(Int,Map[String, String])] = td.config.map { configString =>
          val configJson = Json.parse(configString._2)
          (configString._1,(configJson \ "config").as[Map[String,String]])
        }
        resultOption.getOrElse((-1,Map.empty[String, String]))
      } catch {
        case e: Exception =>
          logger.error(s"Failed to parse topic config : ${td.config.getOrElse("")}",e)
          (-1,Map.empty[String, String])
      }
    }
    TopicIdentity(td.topic,td.description._1,partMap.size,tpi,brokers,config._1,config._2.toList,td.deleteSupported)
  }

  implicit def from(bl: BrokerList,td: TopicDescription) : TopicIdentity = {
    from(bl.list.size, td)
  }

  implicit def reassignReplicas(currentTopicIdentity: TopicIdentity, 
                                assignedReplicas: Map[Int, Seq[Int]]) : Try[TopicIdentity] = {
    Try {
      val newTpi : Map[Int, TopicPartitionIdentity] = currentTopicIdentity.partitionsIdentity.map { case (part, tpi) =>
        val newReplicaSeq = assignedReplicas.get(part)
        require(newReplicaSeq.isDefined, s"Missing replica assignment for partition $part for topic ${currentTopicIdentity.topic}")
        val newReplicaSet = newReplicaSeq.get.toSet
        require(newReplicaSeq.get.size == newReplicaSet.size, s"Duplicates found in replica set ${newReplicaSeq.get} for partition $part for topic ${currentTopicIdentity.topic}")
        (part,tpi.copy(replicas = newReplicaSeq.get))
      }
      TopicIdentity(
        currentTopicIdentity.topic,
        currentTopicIdentity.readVersion,
        currentTopicIdentity.partitions,
        newTpi,
        currentTopicIdentity.numBrokers,
        currentTopicIdentity.configReadVersion,
        currentTopicIdentity.config,
        currentTopicIdentity.deleteSupported)
    }
  }
}
