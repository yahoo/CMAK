/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager.utils

import kafka.manager.BaseTest
import kafka.manager.features.ClusterFeatures
import kafka.manager.model.ActorModel.{PartitionOffsetsCapture, TopicDescription, TopicIdentity}
import kafka.manager.model.{ActorModel, ClusterConfig, ClusterContext, Kafka_0_8_2_0}
import kafka.manager.utils.TopicErrors._
import org.apache.zookeeper.data.Stat

import java.util.Properties
/**
 * @author hiral
 */
class TestCreateTopic extends CuratorAwareTest with BaseTest {
  
  private[this] val adminUtils  = new AdminUtils(Kafka_0_8_2_0)
  private[this] val defaultClusterConfig = ClusterConfig("test","0.8.2.0","localhost:2818",100,false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(defaultTuning), securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
  private[this] val defaultClusterContext = ClusterContext(ClusterFeatures.from(defaultClusterConfig), defaultClusterConfig)

  test("create topic with empty name") {
    checkError[TopicNameEmpty] {
      withCurator { curator =>
        val brokerList = Set(1,2)
        adminUtils.createTopic(curator,brokerList,"",10,2)
      }
    }
  }

  test("create topic with invalid name") {
      withCurator { curator =>
        val brokerList = Set(1,2)
        checkError[InvalidTopicName] {
          adminUtils.createTopic(curator,brokerList,".",10,2)
        }
        checkError[InvalidTopicName] {
          adminUtils.createTopic(curator,brokerList,"..",10,2)
        }
    }
  }

  test("create topic with name too long") {
    checkError[InvalidTopicLength] {
      withCurator { curator =>
        val brokerList = Set(1,2)
        adminUtils.createTopic(curator,brokerList,"adfasfdsafsfasdfsadfasfsdfasffsdfsadfsdfsdfsfasdfdsfdsafasdfsfdsafasdfdsfdsafsdfdsafasdfsdafasdfadsfdsfsdafsdfsadfdsfasfdfasfsdafsdfdsfdsfasdfdsfsdfsadfsdfasdfdsafasdfsadfdfdsfdsfsfsfdsfdsfdssafsdfdsafadfasdfsdafsdfasdffasfdfadsfasdfasfadfafsdfasfdssafffffffffffdsadfsafdasdfsafsfsfsdfafs",10,2)
      }
    }
  }

  test("create topic with bad chars in name") {
    checkError[IllegalCharacterInName] {
      withCurator { curator =>
        val brokerList = Set(1,2)
        adminUtils.createTopic(curator,brokerList,"bad!Topic!",10,2)
      }
    }
  }

  test("create topic with invalid partitions") {
    checkError[PartitionsGreaterThanZero] {
      withCurator { curator =>
        val brokerList = Set(1,2)
        adminUtils.createTopic(curator,brokerList,"mytopic",0,2)
      }
    }
  }

  test("create topic with invalid replication") {
    checkError[ReplicationGreaterThanZero] {
      withCurator { curator =>
        val brokerList = Set(1,2)
        adminUtils.createTopic(curator,brokerList,"mytopic",10,0)
      }
    }
  }

  test("create topic with # of brokers < replication") {
    checkError[ReplicationGreaterThanNumBrokers] {
      withCurator { curator =>
        val brokerList = Set.empty[Int]
        adminUtils.createTopic(curator,brokerList,"mytopic",10,3)
      }
    }
  }

  test("create topic") {
    withCurator { curator =>
      val brokerList = Set(1,2,3)
      val properties = new Properties()
      properties.setProperty(kafka.manager.utils.zero82.LogConfig.RententionMsProp,"1800000")
      adminUtils.createTopic(curator,brokerList,"mytopic",10,3, properties)
      val stat = new Stat()
      val json:String = curator.getData.storingStatIn(stat).forPath(ZkUtils.getTopicPath("mytopic"))
      val configJson : String = curator.getData.forPath(ZkUtils.getTopicConfigPath("mytopic"))
      val td = TopicIdentity.from(3,TopicDescription("mytopic",(stat.getVersion(),json),None,PartitionOffsetsCapture.EMPTY,Option((-1,configJson))),None,None,defaultClusterContext,None)
      assert(td.partitions == 10)
      assert(td.replicationFactor == 3)
    }
  }

  test("create topic - topic already exists") {
    checkError[TopicAlreadyExists] {
      withCurator { curator =>
        val brokerList = Set(1,2,3)
        adminUtils.createTopic(curator, brokerList, "mytopic", 10, 3)
        val json: String = curator.getData.forPath(ZkUtils.getTopicPath("mytopic"))
        assert(json == "{\"version\":1,\"partitions\":{\"8\":[2,3,1],\"4\":[1,3,2],\"9\":[3,2,1],\"5\":[2,1,3],\"6\":[3,1,2],\"1\":[1,2,3],\"0\":[3,1,2],\"2\":[2,3,1],\"7\":[1,2,3],\"3\":[3,2,1]}}")
      }
    }
  }
  
  test("alter topic - cannot add zero partitions") {
    checkError[CannotAddZeroPartitions] {
      withCurator { curator =>
        val brokerList = Set(1,2,3)
        val stat = new Stat
        val json:String = curator.getData.storingStatIn(stat).forPath(ZkUtils.getTopicPath("mytopic"))
        val configJson : String = curator.getData.forPath(ZkUtils.getTopicConfigPath("mytopic"))
        val td = TopicIdentity.from(3,TopicDescription("mytopic",(stat.getVersion,json),None,PartitionOffsetsCapture.EMPTY,Option((-1,configJson))),None,None,defaultClusterContext,None)
        val numPartitions = td.partitions
        adminUtils.addPartitions(curator, td.topic, numPartitions, td.partitionsIdentity.mapValues(_.replicas.toSeq),brokerList, stat.getVersion)
      }
    }
  }

  test("alter topic - replication factor greater than num brokers") {
    checkError[ReplicationGreaterThanNumBrokers] {
      withCurator { curator =>
        val brokerList = Set(1,2)
        val stat = new Stat
        val json:String = curator.getData.storingStatIn(stat).forPath(ZkUtils.getTopicPath("mytopic"))
        val configJson : String = curator.getData.forPath(ZkUtils.getTopicConfigPath("mytopic"))
        val td = TopicIdentity.from(3,TopicDescription("mytopic",(stat.getVersion,json),None,PartitionOffsetsCapture.EMPTY,Option((-1,configJson))),None,None,defaultClusterContext,None)
        val numPartitions = td.partitions + 2
        adminUtils.addPartitions(curator, td.topic, numPartitions, td.partitionsIdentity.mapValues(_.replicas.toSeq),brokerList,stat.getVersion)
      }
    }
  }

  test("alter topic - add partitions") {
    withCurator { curator =>
      val brokerList = Set(1,2,3)
      val stat = new Stat
      val json:String = curator.getData.storingStatIn(stat).forPath(ZkUtils.getTopicPath("mytopic"))
      val configJson : String = curator.getData.forPath(ZkUtils.getTopicConfigPath("mytopic"))
      val td = TopicIdentity.from(3,TopicDescription("mytopic",(stat.getVersion,json),None,PartitionOffsetsCapture.EMPTY,Option((-1,configJson))),None,None,defaultClusterContext,None)
      val numPartitions = td.partitions + 2
      adminUtils.addPartitions(curator, td.topic, numPartitions, td.partitionsIdentity.mapValues(_.replicas.toSeq),brokerList,stat.getVersion)

      //check partitions were added and config updated
      {
        val json: String = curator.getData.forPath(ZkUtils.getTopicPath("mytopic"))
        val configJson: String = curator.getData.forPath(ZkUtils.getTopicConfigPath("mytopic"))
        val td = TopicIdentity.from(3, TopicDescription("mytopic", (-1,json), None, PartitionOffsetsCapture.EMPTY, Option((-1,configJson))),None,None,defaultClusterContext,None)
        assert(td.partitions === numPartitions, "Failed to add partitions!")
        assert(td.config.toMap.apply(kafka.manager.utils.zero82.LogConfig.RententionMsProp) === "1800000")
      }
    }
  }

  test("alter topic - update config") {
    withCurator { curator =>
      val brokerList = Set(1,2,3)
      val stat = new Stat
      val json:String = curator.getData.storingStatIn(stat).forPath(ZkUtils.getTopicPath("mytopic"))
      val configStat = new Stat
      val configJson : String = curator.getData.storingStatIn(configStat).forPath(ZkUtils.getTopicConfigPath("mytopic"))
      val configReadVersion = configStat.getVersion
      val td = TopicIdentity.from(3,TopicDescription("mytopic",(stat.getVersion,json),None,PartitionOffsetsCapture.EMPTY,Option((configReadVersion,configJson))),None,None,defaultClusterContext,None)
      val properties = new Properties()
      td.config.foreach { case (k,v) => properties.put(k,v)}
      properties.setProperty(kafka.manager.utils.zero82.LogConfig.RententionMsProp,"3600000")
      adminUtils.changeTopicConfig(curator, td.topic, properties, configReadVersion)

      //check config
      {
        val json: String = curator.getData.forPath(ZkUtils.getTopicPath("mytopic"))
        val configStat = new Stat
        val configJson : String = curator.getData.storingStatIn(configStat).forPath(ZkUtils.getTopicConfigPath("mytopic"))
        val td = TopicIdentity.from(3, TopicDescription("mytopic", (-1,json), None, PartitionOffsetsCapture.EMPTY, Option((configStat.getVersion,configJson))),None,None,defaultClusterContext,None)
        assert(td.config.toMap.apply(kafka.manager.utils.zero82.LogConfig.RententionMsProp) === "3600000")
        assert(configReadVersion != configStat.getVersion)
      }

      //check config change notification
      {
        import scala.collection.JavaConverters._

        val json: Option[String] = Option(curator.getChildren.forPath(ZkUtils.TopicConfigChangesPath)).map { children =>
          val last = children.asScala.last
          val json : String = curator.getData.forPath(s"${ZkUtils.TopicConfigChangesPath}/$last")
          json
        }

        assert(json.isDefined, "Failed to get data for config change!")
        assert(json.get contains "\"mytopic\"")
      }

    }
  }
}
