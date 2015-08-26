/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import kafka.manager.utils.CuratorAwareTest
import ActorModel.TopicList
import kafka.test.SeededBroker

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/**
 * @author hiral
 */
class TestKafkaManager extends CuratorAwareTest {
  private[this] val broker = new SeededBroker("km-api-test",4)
  private[this] val kafkaServerZkPath = broker.getZookeeperConnectionString
  private[this] val akkaConfig: Properties = new Properties()
  akkaConfig.setProperty("pinned-dispatcher.type","PinnedDispatcher")
  akkaConfig.setProperty("pinned-dispatcher.executor","thread-pool-executor")
  akkaConfig.setProperty(KafkaManager.ZkHosts,testServer.getConnectString)
  akkaConfig.setProperty(KafkaManager.BrokerViewUpdateSeconds,"1")
  akkaConfig.setProperty(KafkaManager.KafkaManagerUpdateSeconds,"1")
  akkaConfig.setProperty(KafkaManager.DeleteClusterUpdateSeconds,"1")
  private[this] val config : Config = ConfigFactory.parseProperties(akkaConfig)

  private[this] val kafkaManager : KafkaManager = new KafkaManager(config)

  private[this] val duration = FiniteDuration(10,SECONDS)
  private[this] val createTopicNameA = "km-unit-test-a"
  private[this] val createTopicNameB = "km-unit-test-b"

  override protected def beforeAll() : Unit = {
    super.beforeAll()
    Thread.sleep(1000)
  }

  override protected def afterAll(): Unit = {
    kafkaManager.shutdown()
    Try(broker.shutdown())
    super.afterAll()
  }

  private[this] def getTopicList() : TopicList = {
    val future = kafkaManager.getTopicList("dev")
    val result = Await.result(future,duration)
    result.toOption.get
  }

  test("add cluster") {
    val future = kafkaManager.addCluster("dev","0.8.2.0",kafkaServerZkPath, jmxEnabled = false, filterConsumers = true)
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    Thread.sleep(2000)
  }

  test("create topic") {
    val futureA = kafkaManager.createTopic("dev",createTopicNameA,4,1)
    val resultA = Await.result(futureA,duration)
    val futureB = kafkaManager.createTopic("dev",createTopicNameB,4,1)
    val resultB = Await.result(futureB,duration)
    assert(resultA.isRight === true)
    assert(resultB.isRight === true)
    Thread.sleep(2000)
  }

  test("fail to create topic again") {
    val future = kafkaManager.createTopic("dev",createTopicNameA,4,1)
    val result = Await.result(future,duration)
    assert(result.isLeft === true)
    Thread.sleep(2000)
  }

  test("get topic list") {
    val future = kafkaManager.getTopicList("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    assert(result.toOption.get.list.nonEmpty === true)
  }

  test("query request for invalid cluster") {
    val future = kafkaManager.getTopicList("blah")
    val result = Await.result(future,duration)
    assert(result.isLeft === true)
    assert(result.swap.toOption.get.msg.contains("blah") === true)
  }

  test("get broker list") {
    val future = kafkaManager.getBrokerList("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    assert(result.toOption.nonEmpty === true)
  }

  test("get topic identity") {
    val future = kafkaManager.getTopicList("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    assert(result.toOption.get.list.nonEmpty === true)
    result.toOption.get.list.foreach { topic =>
      val future2 = kafkaManager.getTopicIdentity("dev",topic)
      val result2 = Await.result(future2, duration)
      assert(result2.isRight === true)
    }
  }

  test("get cluster list") {
    val future = kafkaManager.getClusterList
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    assert(result.toOption.get.active.nonEmpty === true)
  }

  test("get cluster view") {
    val future = kafkaManager.getClusterView("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
  }

  test("get cluster config") {
    val future = kafkaManager.getClusterConfig("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
  }

  test("run preferred leader election") {
    val topicList = getTopicList()
    val future = kafkaManager.runPreferredLeaderElection("dev",topicList.list.toSet)
    val result = Await.result(future,duration)
    //TODO: this is a failure since there is nothing to do, need a better test
    assert(result.isLeft === true)
    Thread.sleep(3000)
  }

  test("get preferred leader election") {
    val future = kafkaManager.getPreferredLeaderElection("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    println(result.toOption.get)
  }

  test("generate partition assignments") {
    val topicList = getTopicList()
    val future = kafkaManager.generatePartitionAssignments("dev",topicList.list.toSet,Seq(0))
    val result = Await.result(future,duration)
    assert(result.isRight === true)
  }

  test("run reassign partitions") {
    val topicList = getTopicList()
    val future = kafkaManager.runReassignPartitions("dev",topicList.list.toSet)
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    Thread.sleep(3000)
  }

  test("get reassign partitions") {
    val future = kafkaManager.getReassignPartitions("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
  }

  test("add topic partitions") {
    val tiFuture= kafkaManager.getTopicIdentity("dev",createTopicNameA)
    val tiOrError = Await.result(tiFuture, duration)
    assert(tiOrError.isRight, "Failed to get topic identity!")
    val ti = tiOrError.toOption.get
    val future = kafkaManager.addTopicPartitions("dev",createTopicNameA,Seq(0),ti.partitions + 1,ti.readVersion)
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    
    //check new partition num
    {
      val tiFuture= kafkaManager.getTopicIdentity("dev",createTopicNameA)
      val tiOrError = Await.result(tiFuture, duration)
      assert(tiOrError.isRight, "Failed to get topic identity!")
      val ti = tiOrError.toOption.get
      assert(ti.partitions === 5)
    }
  }

  test("add multiple topics partitions") {
    val tiFutureA = kafkaManager.getTopicIdentity("dev",createTopicNameA)
    val tiFutureB = kafkaManager.getTopicIdentity("dev",createTopicNameB)
    val tiOrErrorA = Await.result(tiFutureA,duration)
    val tiOrErrorB = Await.result(tiFutureB,duration)
    assert(tiOrErrorA.isRight, "Failed to get topic identity for topic A!")
    assert(tiOrErrorB.isRight, "Failed to get topic identity for topic B!")
    val tiA = tiOrErrorA.toOption.get
    val tiB = tiOrErrorB.toOption.get
    val newPartitionNum = tiA.partitions + 1
    val future = kafkaManager.addMultipleTopicsPartitions("dev",Seq(createTopicNameA, createTopicNameB),Seq(0),newPartitionNum,Map(createTopicNameA->tiA.readVersion,createTopicNameB->tiB.readVersion))
    val result = Await.result(future,duration)
    assert(result.isRight === true)

    {
      val tiFutureA = kafkaManager.getTopicIdentity("dev",createTopicNameA)
      val tiFutureB = kafkaManager.getTopicIdentity("dev",createTopicNameB)
      val tiOrErrorA = Await.result(tiFutureA,duration)
      val tiOrErrorB = Await.result(tiFutureB,duration)
      assert(tiOrErrorA.isRight, "Failed to get topic identity for topic A!")
      assert(tiOrErrorB.isRight, "Failed to get topic identity for topic B!")
      val tiA = tiOrErrorA.toOption.get
      val tiB = tiOrErrorB.toOption.get
      assert(tiA.partitions === newPartitionNum)
      assert(tiB.partitions === newPartitionNum)
    }
  }

  test("update topic config") {
    val tiFuture= kafkaManager.getTopicIdentity("dev",createTopicNameA)
    val tiOrError = Await.result(tiFuture, duration)
    assert(tiOrError.isRight, "Failed to get topic identity!")
    val ti = tiOrError.toOption.get
    val config = new Properties()
    config.put(kafka.manager.utils.zero82.LogConfig.RententionMsProp,"1800000")
    val configReadVersion = ti.configReadVersion
    val future = kafkaManager.updateTopicConfig("dev",createTopicNameA,config,configReadVersion)
    val result = Await.result(future,duration)
    assert(result.isRight === true)

    //check new topic config
    {
      val tiFuture= kafkaManager.getTopicIdentity("dev",createTopicNameA)
      val tiOrError = Await.result(tiFuture, duration)
      assert(tiOrError.isRight, "Failed to get topic identity!")
      val ti = tiOrError.toOption.get
      assert(ti.configReadVersion > configReadVersion)
      assert(ti.config.toMap.apply(kafka.manager.utils.zero82.LogConfig.RententionMsProp) === "1800000")
    }
  }

  test("delete topic") {
    val futureA = kafkaManager.deleteTopic("dev",createTopicNameA)
    val resultA = Await.result(futureA,duration)
    assert(resultA.isRight === true, resultA)
    val futureA2 = kafkaManager.getTopicList("dev")
    val resultA2 = Await.result(futureA2,duration)
    assert(resultA2.isRight === true, resultA2)
    assert(resultA2.toOption.get.deleteSet(createTopicNameA),"Topic not in delete set")

    val futureB = kafkaManager.deleteTopic("dev",createTopicNameB)
    val resultB = Await.result(futureB,duration)
    assert(resultB.isRight === true, resultB)
    val futureB2 = kafkaManager.getTopicList("dev")
    val resultB2 = Await.result(futureB2,duration)
    assert(resultB2.isRight === true, resultB2)
    assert(resultB2.toOption.get.deleteSet(createTopicNameB),"Topic not in delete set")
    Thread.sleep(2000)
  }

  test("fail to delete non-existent topic") {
    val future = kafkaManager.deleteTopic("dev","delete_me")
    val result = Await.result(future,duration)
    assert(result.isLeft === true)
  }

  test("update cluster zkhost") {
    val future = kafkaManager.updateCluster("dev","0.8.2.0",testServer.getConnectString, jmxEnabled = false, filterConsumers = true)
    val result = Await.result(future,duration)
    assert(result.isRight === true)

    val future2 = kafkaManager.getClusterList
    val result2 = Await.result(future2,duration)
    assert(result2.isRight === true)
    assert((result2.toOption.get.pending.nonEmpty === true) ||
           (result2.toOption.get.active.find(c => c.name == "dev").get.curatorConfig.zkConnect === testServer.getConnectString))
    Thread.sleep(3000)
  }

  test("disable cluster") {
    val future = kafkaManager.disableCluster("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)

    val future2 = kafkaManager.getClusterList
    val result2 = Await.result(future2,duration)
    assert(result2.isRight === true)
    assert((result2.toOption.get.pending.nonEmpty === true) ||
           (result2.toOption.get.active.find(c => c.name == "dev").get.enabled === false))
    Thread.sleep(3000)
  }

  test("enable cluster") {
    val future = kafkaManager.enableCluster("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    Thread.sleep(3000)
  }

  test("update cluster version") {
    val future = kafkaManager.updateCluster("dev","0.8.1.1",testServer.getConnectString, jmxEnabled = false, filterConsumers = true)
    val result = Await.result(future,duration)
    assert(result.isRight === true)

    val future2 = kafkaManager.getClusterList
    val result2 = Await.result(future2,duration)
    assert(result2.isRight === true)
    assert((result2.toOption.get.pending.nonEmpty === true) ||
           (result2.toOption.get.active.find(c => c.name == "dev").get.version === Kafka_0_8_1_1))
    Thread.sleep(5000)
  }

  test("delete topic not supported prior to 0.8.2.0") {
    val future = kafkaManager.deleteTopic("dev",createTopicNameA)
    val result = Await.result(future,duration)
    assert(result.isLeft === true, result)
    assert(result.swap.toOption.get.msg.contains("not supported"))
    Thread.sleep(2000)
  }

  test("delete cluster") {
    //first have to disable in order to delete
    {
      val future = kafkaManager.disableCluster("dev")
      val result = Await.result(future, duration)
      assert(result.isRight === true)
      Thread.sleep(3000)
    }

    val future = kafkaManager.deleteCluster("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    Thread.sleep(3000)
    val future2 = kafkaManager.getClusterList
    val result2 = Await.result(future2,duration)
    assert(result2.isRight === true)
    assert(result2.toOption.get.pending.isEmpty === true)
    assert(result2.toOption.get.active.isEmpty === true)
  }
}
