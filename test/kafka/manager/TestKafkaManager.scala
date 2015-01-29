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
  private[this] val createTopicName = "km-unit-test"

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
    val future = kafkaManager.addCluster("dev","0.8.1.1",kafkaServerZkPath)
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    Thread.sleep(2000)
  }

  test("create topic") {
    val future = kafkaManager.createTopic("dev",createTopicName,4,1)
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    Thread.sleep(2000)
  }

  test("fail to create topic again") {
    val future = kafkaManager.createTopic("dev",createTopicName,4,1)
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
    assert(result.toOption.get.nonEmpty === true)
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
    val future = kafkaManager.generatePartitionAssignments("dev",topicList.list.toSet)
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

  test("delete topic") {
    val future = kafkaManager.deleteTopic("dev",createTopicName)
    val result = Await.result(future,duration)
    assert(result.isRight === true, result)
    Thread.sleep(2000)
  }

  test("fail to delete non-existent topic") {
    val future = kafkaManager.deleteTopic("dev","delete_me")
    val result = Await.result(future,duration)
    assert(result.isLeft === true)
  }

  test("update cluster zkhost") {
    val future = kafkaManager.updateCluster("dev","0.8.1.1",testServer.getConnectString)
    val result = Await.result(future,duration)
    assert(result.isRight === true)

    val future2 = kafkaManager.getClusterList
    val result2 = Await.result(future2,duration)
    assert(result2.isRight === true)
    assert(result2.toOption.get.pending.nonEmpty === true)
    Thread.sleep(3000)
  }

  test("disable cluster") {
    val future = kafkaManager.disableCluster("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)

    val future2 = kafkaManager.getClusterList
    val result2 = Await.result(future2,duration)
    assert(result2.isRight === true)
    assert(result2.toOption.get.pending.nonEmpty === true)
    Thread.sleep(3000)
  }

  test("enable cluster") {
    val future = kafkaManager.disableCluster("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    Thread.sleep(3000)
  }

  test("update cluster version") {
    val future = kafkaManager.updateCluster("dev","0.8.2-beta",testServer.getConnectString)
    val result = Await.result(future,duration)
    assert(result.isRight === true)

    val future2 = kafkaManager.getClusterList
    val result2 = Await.result(future2,duration)
    assert(result2.isRight === true)
    assert(result2.toOption.get.pending.nonEmpty === true)
    Thread.sleep(3000)
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
