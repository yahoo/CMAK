/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controller.api

import controllers.KafkaManagerContext
import controllers.api.KafkaHealthCheck
import kafka.manager.utils.{CuratorAwareTest, KafkaServerInTest}
import kafka.test.SeededBroker
import play.api.Play
import play.api.libs.json.Json
import play.api.test.Helpers._
import play.api.test.{FakeApplication, FakeRequest}
import play.mvc.Http.Status.{BAD_REQUEST, OK}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try


class TestKafkaHealthCheck extends CuratorAwareTest with KafkaServerInTest {
  private[this] val broker = new SeededBroker("controller-api-test",4)
  override val kafkaServerZkPath = broker.getZookeeperConnectionString
  private[this] val duration = FiniteDuration(10,SECONDS)

  private[this] val testClusterName = "kafka-hc-test-cluster"
  private[this] val testTopicName = "kafka-hc-test-topic"

  override protected def beforeAll() : Unit = {
    super.beforeAll()
    lazy val app : FakeApplication = {
      FakeApplication(additionalConfiguration = Map("kafka-manager.zkhosts" -> kafkaServerZkPath))
    }
    Play.start(app)
    createCluster()
    createTopic()
    Thread.sleep(10000)

  }

  override protected def afterAll(): Unit = {
    disableCluster()
    deleteCluster()
    Play.stop()
    Try(broker.shutdown())
    super.afterAll()
  }

  private[this] def createCluster() = {
    val future = KafkaManagerContext.getKafkaManager.addCluster(testClusterName,"0.8.2.0",kafkaServerZkPath, jmxEnabled = false, filterConsumers = true)
    val result = Await.result(future,duration)
    result.toEither.left.foreach(apiError => sys.error(apiError.msg))
    Thread.sleep(3000)
  }

  private[this] def createTopic() = {
    val future = KafkaManagerContext.getKafkaManager.createTopic(testClusterName,testTopicName,4,1)
    val result = Await.result(future,duration)
    result.toEither.left.foreach(apiError => sys.error(apiError.msg))
  }

  private[this] def deleteTopic() = {
    val future = KafkaManagerContext.getKafkaManager.deleteTopic(testClusterName,testTopicName)
    val result = Await.result(future,duration)
  }

  private[this] def disableCluster() = {
    val future = KafkaManagerContext.getKafkaManager.disableCluster(testClusterName)
    Await.result(future, duration)
    Thread.sleep(3000)
  }
  private[this] def deleteCluster() = {
    val future = KafkaManagerContext.getKafkaManager.deleteCluster(testClusterName)
    Await.result(future,duration)
    Thread.sleep(3000)
  }

  test("get available brokers") {
    val future = KafkaHealthCheck.availableBrokers(testClusterName).apply(FakeRequest())
    assert(status(future) === OK)
    assert(contentAsJson(future) === Json.obj("availableBrokers" -> Seq(0)))
  }

  test("get available brokers in non-existing cluster") {
    val future = KafkaHealthCheck.availableBrokers("non-existent").apply(FakeRequest())
    assert(status(future) === BAD_REQUEST)
  }

  test("get under-replicated partitions") {
    val future = KafkaHealthCheck.underReplicatedPartitions(testClusterName, testTopicName).apply(FakeRequest())
    assert(status(future) === OK)
    assert(contentAsJson(future) === Json.obj("topic" -> testTopicName, "underReplicatedPartitions" -> Seq.empty[Int]))
  }

  test("get under-replicated partitions of non-existing topic in non-existing cluster") {
    val future = KafkaHealthCheck.underReplicatedPartitions("non-existent", "weird").apply(FakeRequest())
    assert(status(future) === BAD_REQUEST)
  }

  test("get unavailable partitions") {
    val future = KafkaHealthCheck.unavailablePartitions(testClusterName, testTopicName).apply(FakeRequest())
    assert(status(future) == OK)
    assert(contentAsJson(future) == Json.obj("topic" -> testTopicName, "unavailablePartitions" -> Seq.empty[Int]))
  }

  test("get unavailable partitions of non-existing topic in non-existing cluster") {
    val future = KafkaHealthCheck.unavailablePartitions("non-existent", "weird").apply(FakeRequest())
    assert(status(future) === BAD_REQUEST)
  }
}
