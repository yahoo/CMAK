/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager.utils

import kafka.manager.ActorModel._
import kafka.manager.TopicIdentity

/**
 * @author hiral
 */
class TestReassignPartitions extends CuratorAwareTest {

  import ReassignPartitionErrors._

  private[this] val brokerList = IndexedSeq(1,2,3)

  private[this] def mytopic1 : TopicIdentity = getTopicIdentity("mytopic1")
  private[this] def mytopic2 : TopicIdentity = getTopicIdentity("mytopic2")
  private[this] def mytopic3 : TopicIdentity = getTopicIdentity("mytopic3")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    withCurator { curator =>
      AdminUtils.createTopic(curator,brokerList,"mytopic1",3,3)
      AdminUtils.createTopic(curator,brokerList,"mytopic2",6,3)
      AdminUtils.createTopic(curator,brokerList,"mytopic3",9,3)
    }
  }

  private[this] def getTopicIdentity(topic: String): TopicIdentity = {
    produceWithCurator { curator =>
      val json : String = curator.getData.forPath(ZkUtils.getTopicPath(topic))
      val td: TopicDescription = TopicDescription(topic,json,None)
      TopicIdentity.from(brokerList.size,td)
    }
  }

  test("reassign partitions with empty set") {
    withCurator { curator =>
      assert(ReassignPartitionCommand.executeAssignment(curator,Map.empty, Map.empty).isFailure)
      assert(curator.checkExists().forPath(ZkUtils.ReassignPartitionsPath) == null)
    }
  }

  test("reassign partitions with out of sync partition count") {
    checkError[PartitionsOutOfSync] {
      withCurator { curator =>
        val current = Map("mytopic1" -> mytopic1, "mytopic2" -> mytopic2, "mytopic3" -> mytopic3)
        val generated = current.map { case (t,td) =>
          (t,ReassignPartitionCommand.generateAssignment(
            brokerList,
            td.copy(partitions = td.partitions - 1, partitionsIdentity = td.partitionsIdentity - (td.partitions - 1))).get)
        }

        ReassignPartitionCommand.executeAssignment(curator,current,generated).get
      }
    }
  }

  test("reassign partitions with out of sync replication factor") {
    checkError[ReplicationOutOfSync] {
      withCurator { curator =>
        val current = Map("mytopic1" -> mytopic1, "mytopic2" -> mytopic2, "mytopic3" -> mytopic3)
        val generated = current.map { case (t,td) =>
          (t,ReassignPartitionCommand.generateAssignment(
            brokerList,
            td.copy(partitionsIdentity = td.partitionsIdentity.map { case (p,l) => (p, l.copy(replicas = l.replicas.drop(1)))})).get)
        }

        ReassignPartitionCommand.executeAssignment(curator,current,generated).get
      }
    }
  }

  test("reassign partitions") {
    withCurator { curator =>
      val current = Map("mytopic1" -> mytopic1, "mytopic2" -> mytopic2, "mytopic3" -> mytopic3)
      val generated = current.map { case (t,td) =>
        (t,ReassignPartitionCommand.generateAssignment(
          brokerList,
          td).get)
      }

      assert(ReassignPartitionCommand.executeAssignment(curator,current,generated).isSuccess)
    }
  }

  test("reassign partitions already running") {
    checkError[ReassignmentAlreadyInProgress] {
      withCurator { curator =>
        val current = Map("mytopic1" -> mytopic1, "mytopic2" -> mytopic2, "mytopic3" -> mytopic3)
        val generated = current.map { case (t,td) =>
          (t,ReassignPartitionCommand.generateAssignment(
            brokerList,
            td).get)
        }

        ReassignPartitionCommand.executeAssignment(curator,current,generated).get
      }
    }
  }
}
