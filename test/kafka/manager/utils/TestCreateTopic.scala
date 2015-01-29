/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager.utils

import TopicErrors._
import kafka.manager.ActorModel.TopicDescription
import kafka.manager.TopicIdentity

/**
 * @author hiral
 */
class TestCreateTopic extends CuratorAwareTest {

  test("create topic with empty name") {
    checkError[TopicNameEmpty] {
      withCurator { curator =>
        val brokerList = IndexedSeq(1,2)
        AdminUtils.createTopic(curator,brokerList,"",10,2)
      }
    }
  }

  test("create topic with invalid name") {
      withCurator { curator =>
        val brokerList = IndexedSeq(1,2)
        checkError[InvalidTopicName] {
          AdminUtils.createTopic(curator,brokerList,".",10,2)
        }
        checkError[InvalidTopicName] {
          AdminUtils.createTopic(curator,brokerList,"..",10,2)
        }
    }
  }

  test("create topic with name too long") {
    checkError[InvalidTopicLength] {
      withCurator { curator =>
        val brokerList = IndexedSeq(1,2)
        AdminUtils.createTopic(curator,brokerList,"adfasfdsafsfasdfsadfasfsdfasffsdfsadfsdfsdfsfasdfdsfdsafasdfsfdsafasdfdsfdsafsdfdsafasdfsdafasdfadsfdsfsdafsdfsadfdsfasfdfasfsdafsdfdsfdsfasdfdsfsdfsadfsdfasdfdsafasdfsadfdfdsfdsfsfsfdsfdsfdssafsdfdsafadfasdfsdafsdfasdffasfdfadsfasdfasfadfafsdfasfdssafffffffffffdsadfsafdasdfsafsfsfsdfafs",10,2)
      }
    }
  }

  test("create topic with bad chars in name") {
    checkError[IllegalCharacterInName] {
      withCurator { curator =>
        val brokerList = IndexedSeq(1,2)
        AdminUtils.createTopic(curator,brokerList,"bad!Topic!",10,2)
      }
    }
  }

  test("create topic with invalid partitions") {
    checkError[PartitionsGreaterThanZero] {
      withCurator { curator =>
        val brokerList = IndexedSeq(1,2)
        AdminUtils.createTopic(curator,brokerList,"mytopic",0,2)
      }
    }
  }

  test("create topic with invalid replication") {
    checkError[ReplicationGreaterThanZero] {
      withCurator { curator =>
        val brokerList = IndexedSeq(1,2)
        AdminUtils.createTopic(curator,brokerList,"mytopic",10,0)
      }
    }
  }

  test("create topic with # of brokers < replication") {
    checkError[ReplicationGreaterThanNumBrokers] {
      withCurator { curator =>
        val brokerList = IndexedSeq.empty[Int]
        AdminUtils.createTopic(curator,brokerList,"mytopic",10,3)
      }
    }
  }

  test("create topic") {
    withCurator { curator =>
      val brokerList = IndexedSeq(1,2,3)
      AdminUtils.createTopic(curator,brokerList,"mytopic",10,3)
      val json:String = curator.getData.forPath(ZkUtils.getTopicPath("mytopic"))
      val td = TopicIdentity.from(3,TopicDescription("mytopic",json,None))
      assert(td.partitions == 10)
      assert(td.replicationFactor == 3)
    }
  }

  test("create topic - topic already exists") {
    checkError[TopicAlreadyExists] {
      withCurator { curator =>
        val brokerList = IndexedSeq(1,2,3)
        AdminUtils.createTopic(curator, brokerList, "mytopic", 10, 3)
        val json: String = curator.getData.forPath(ZkUtils.getTopicPath("mytopic"))
        assert(json == "{\"version\":1,\"partitions\":{\"8\":[2,3,1],\"4\":[1,3,2],\"9\":[3,2,1],\"5\":[2,1,3],\"6\":[3,1,2],\"1\":[1,2,3],\"0\":[3,1,2],\"2\":[2,3,1],\"7\":[1,2,3],\"3\":[3,2,1]}}")
      }
    }
  }
}
