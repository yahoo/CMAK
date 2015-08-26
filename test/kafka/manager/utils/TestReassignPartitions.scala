/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager.utils

import java.util.Properties

import kafka.manager.ActorModel._
import kafka.manager.{ClusterConfig, Kafka_0_8_2_0}
import kafka.manager.utils.zero81._
import org.apache.zookeeper.data.Stat

import scala.concurrent.Future

/**
 * @author hiral
 */
class TestReassignPartitions extends CuratorAwareTest {

  import ReassignPartitionErrors._

  private[this] val adminUtils  = new AdminUtils(Kafka_0_8_2_0)
  
  private[this] val reassignPartitionCommand = new ReassignPartitionCommand(adminUtils)

  private[this] val brokerList = IndexedSeq(1,2,3)

  private[this] val defaultClusterConfig = ClusterConfig("test","0.8.2.0","localhost:2818",100,false,true)
  
  private[this] def mytopic1 : TopicIdentity = getTopicIdentity("mytopic1")
  private[this] def mytopic2 : TopicIdentity = getTopicIdentity("mytopic2")
  private[this] def mytopic3 : TopicIdentity = getTopicIdentity("mytopic3")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    withCurator { curator =>
      val properties = new Properties()
      properties.put(LogConfig.RententionMsProp,"86400000")
      adminUtils.createTopic(curator,brokerList,"mytopic1",3,3,properties)
      adminUtils.createTopic(curator,brokerList,"mytopic2",6,3)
      adminUtils.createTopic(curator,brokerList,"mytopic3",9,3)
    }
  }

  private[this] def getTopicIdentity(topic: String): TopicIdentity = {
    produceWithCurator { curator =>
      val stat = new Stat
      val json : String = curator.getData.storingStatIn(stat).forPath(ZkUtils.getTopicPath(topic))
      val configStat = new Stat
      val configJson : String = curator.getData.storingStatIn(configStat).forPath(ZkUtils.getTopicConfigPath(topic))
      val td: TopicDescription = TopicDescription(topic,(stat.getVersion,json),None,Future.successful(Map.empty),Option((configStat.getVersion,configJson)),false)
      TopicIdentity.from(brokerList.size,td,None, defaultClusterConfig)
    }
  }

  test("reassign partitions with empty set") {
    withCurator { curator =>
      assert(reassignPartitionCommand.executeAssignment(curator,Map.empty, Map.empty).isFailure)
      assert(curator.checkExists().forPath(ZkUtils.ReassignPartitionsPath) == null)
    }
  }

  test("reassign partitions with out of sync partition count") {
    checkError[PartitionsOutOfSync] {
      withCurator { curator =>
        val current = Map("mytopic1" -> mytopic1, "mytopic2" -> mytopic2, "mytopic3" -> mytopic3)
        val generated = current.map { case (t,td) =>
          (t,reassignPartitionCommand.generateAssignment(
            brokerList,
            td.copy(partitions = td.partitions - 1, partitionsIdentity = td.partitionsIdentity - (td.partitions - 1))).get)
        }

        reassignPartitionCommand.executeAssignment(curator,current,generated).get
      }
    }
  }

  test("reassign partitions with out of sync replication factor") {
    checkError[ReplicationOutOfSync] {
      withCurator { curator =>
        val current = Map("mytopic1" -> mytopic1, "mytopic2" -> mytopic2, "mytopic3" -> mytopic3)
        val generated = current.map { case (t,td) =>
          (t,reassignPartitionCommand.generateAssignment(
            brokerList,
            td.copy(partitionsIdentity = td.partitionsIdentity.map { case (p,l) => (p, l.copy(replicas = l.replicas.drop(1)))})).get)
        }

        reassignPartitionCommand.executeAssignment(curator,current,generated).get
      }
    }
  }

  test("reassign partitions") {
    withCurator { curator =>
      val current = Map("mytopic1" -> mytopic1, "mytopic2" -> mytopic2, "mytopic3" -> mytopic3)
      val generated = current.map { case (t,td) =>
        (t,reassignPartitionCommand.generateAssignment(
          brokerList,
          td).get)
      }

      assert(reassignPartitionCommand.executeAssignment(curator,current,generated).isSuccess)
    }
  }

  test("reassign partitions already running") {
    checkError[ReassignmentAlreadyInProgress] {
      withCurator { curator =>
        val current = Map("mytopic1" -> mytopic1, "mytopic2" -> mytopic2, "mytopic3" -> mytopic3)
        val generated = current.map { case (t,td) =>
          (t,reassignPartitionCommand.generateAssignment(
            brokerList,
            td).get)
        }

        reassignPartitionCommand.executeAssignment(curator,current,generated).get
      }
    }
  }
}
