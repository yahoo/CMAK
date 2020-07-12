/**
  * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
  * See accompanying LICENSE file.
  */

package controller.api

import java.io.File
import java.util.Properties

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import controllers.KafkaManagerContext
import controllers.api.KafkaStateCheck
import kafka.manager.KafkaManager
import kafka.manager.utils.{CuratorAwareTest, KafkaServerInTest}
import kafka.test.SeededBroker
import loader.KafkaManagerLoaderForTests
import org.scalatest.Matchers._
import org.scalatest.mockito.MockitoSugar
import play.api.libs.json.{JsDefined, Json}
import play.api.test.FakeRequest
import play.api.test.Helpers._
import play.api.{Application, ApplicationLoader, Environment, Mode}
import play.mvc.Http.Status.{BAD_REQUEST, OK}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try

class TestKafkaStateCheck extends CuratorAwareTest with KafkaServerInTest with MockitoSugar {
  private[this] val broker = new SeededBroker("controller-api-test", 4)
  override val kafkaServerZkPath = broker.getZookeeperConnectionString
  private[this] val duration = FiniteDuration(10, SECONDS)

  private[this] val testClusterName = "kafka-sc-test-cluster"
  private[this] val testTopicName = "kafka-sc-test-topic"
  private[this] var kafkaManagerContext: Option[KafkaManagerContext] = None
  private[this] var kafkaStateCheck: Option[KafkaStateCheck] = None
  private[this] var application: Option[Application] = None

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val configMap: Map[String, AnyRef] = Map(
      "pinned-dispatcher.type" -> "PinnedDispatcher",
      "pinned-dispatcher.executor" -> "thread-pool-executor",
      "cmak.zkhosts" -> kafkaServerZkPath,
      "cmak.consumer.properties.file" -> "conf/consumer.properties"
    )
    val loader = new KafkaManagerLoaderForTests
    application = Option(loader.load(ApplicationLoader.createContext(
      Environment(new File("app"), Thread.currentThread().getContextClassLoader, Mode.Test)
      , configMap
    )))

    val kmc = loader.kafkaManagerContext
    implicit val af = loader.applicationFeatures
    implicit val menus = loader.menus
    implicit val executionContext: ExecutionContext = loader.executionContext
    kafkaManagerContext = Option(kmc)
    val ksc = loader.kafkaStateCheck
    kafkaStateCheck = Option(ksc)
    createCluster()
    createTopic()
    Thread.sleep(10000)

  }

  override protected def afterAll(): Unit = {
    disableCluster()
    deleteCluster()
    Try(application.foreach(app => Await.result(app.stop(), Duration.apply("5s"))))
    kafkaManagerContext.foreach(_.getKafkaManager.shutdown())
    Try(broker.shutdown())
    super.afterAll()
  }

  private[this] def createCluster() = {
    val future = kafkaManagerContext.get.getKafkaManager.addCluster(
      testClusterName, "2.4.1", kafkaServerZkPath, jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(kafkaManagerContext.get.getKafkaManager.defaultTuning), securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None
    )
    val result = Await.result(future, duration)
    result.toEither.left.foreach(apiError => sys.error(apiError.msg))
    Thread.sleep(3000)
  }

  private[this] def createTopic() = {
    val future = kafkaManagerContext.get.getKafkaManager.createTopic(testClusterName, testTopicName, 4, 1)
    val result = Await.result(future, duration)
    result.toEither.left.foreach(apiError => sys.error(apiError.msg))
  }

  private[this] def deleteTopic() = {
    val future = kafkaManagerContext.get.getKafkaManager.deleteTopic(testClusterName, testTopicName)
    val result = Await.result(future, duration)
  }

  private[this] def disableCluster() = {
    val future = kafkaManagerContext.get.getKafkaManager.disableCluster(testClusterName)
    Await.result(future, duration)
    Thread.sleep(3000)
  }

  private[this] def deleteCluster() = {
    val future = kafkaManagerContext.get.getKafkaManager.deleteCluster(testClusterName)
    Await.result(future, duration)
    Thread.sleep(3000)
  }

  test("get brokers") {
    val future = kafkaStateCheck.get.brokers(testClusterName).apply(FakeRequest())
    assert(status(future) === OK)
    assert(contentAsJson(future) === Json.obj("brokers" -> Seq(0)))
  }

  test("get available brokers in non-existing cluster") {
    val future = kafkaStateCheck.get.brokers("non-existent").apply(FakeRequest())
    assert(status(future) === BAD_REQUEST)
  }

  test("get topics") {
    val future = kafkaStateCheck.get.topics(testClusterName).apply(FakeRequest())
    assert(status(future) === OK)
    assert(contentAsJson(future) === Json.obj("topics" -> Seq(testTopicName, "controller-api-test", "__consumer_offsets").sorted))
  }

  test("get topics in non-existing cluster") {
    val future = kafkaStateCheck.get.topics("non-existent").apply(FakeRequest())
    assert(status(future) === BAD_REQUEST)
  }

  test("get under-replicated partitions") {
    val future = kafkaStateCheck.get.underReplicatedPartitions(testClusterName, testTopicName).apply(FakeRequest())
    assert(status(future) === OK)
    assert(contentAsJson(future) === Json.obj("topic" -> testTopicName, "underReplicatedPartitions" -> Seq.empty[Int]))
  }

  test("get under-replicated partitions of non-existing topic in non-existing cluster") {
    val future = kafkaStateCheck.get.underReplicatedPartitions("non-existent", "weird").apply(FakeRequest())
    assert(status(future) === BAD_REQUEST)
  }

  test("get unavailable partitions") {
    val future = kafkaStateCheck.get.unavailablePartitions(testClusterName, testTopicName).apply(FakeRequest())
    assert(status(future) == OK)
    assert(contentAsJson(future) == Json.obj("topic" -> testTopicName, "unavailablePartitions" -> Seq.empty[Int]))
  }

  test("get unavailable partitions of non-existing topic in non-existing cluster") {
    val future = kafkaStateCheck.get.unavailablePartitions("non-existent", "weird").apply(FakeRequest())
    assert(status(future) === BAD_REQUEST)
  }

  test("topic summary") {
    val future = kafkaStateCheck.get.topicSummaryAction(testClusterName, "null", testTopicName, "KF").apply(FakeRequest())
    assert(status(future) === OK)
    val json = Json.parse(contentAsJson(future).toString())
    //(json \ "totalLag").asOpt[Int] should not be empty
    (json \ "percentageCovered").asOpt[Int] should not be empty
    (json \ "partitionOffsets").asOpt[Seq[Long]] should not be empty
    (json \ "partitionLatestOffsets").asOpt[Seq[Long]] should not be empty
    (json \ "owners").asOpt[Seq[String]] should not be empty
  }

  test("get unavailable topic summary") {
    val future = kafkaStateCheck.get.topicSummaryAction("non-existent", "null", "weird", "KF").apply(FakeRequest())
    assert(status(future) === BAD_REQUEST)

  }

  test("get unavailable group summary") {
    val future = kafkaStateCheck.get.groupSummaryAction("non-existent", "weird", "KF").apply(FakeRequest())
    assert(status(future) === BAD_REQUEST)
  }

  test("get clusters") {
    val future = kafkaStateCheck.get.clusters.apply(FakeRequest())
    assert(status(future) === OK)
    val json = Json.parse(contentAsJson(future).toString())
    println(Json.prettyPrint(json))
    assert((json \ "clusters").isInstanceOf[JsDefined])
  }

  test("get topic identities") {
    val future = kafkaStateCheck.get.topicIdentities(testClusterName).apply(FakeRequest())
    assert(status(future) === OK)
    val json = Json.parse(contentAsJson(future).toString())
    println(Json.prettyPrint(json))
    assert((json \ "topicIdentities").isInstanceOf[JsDefined])
  }

  test("consumers summary") {
    val future = kafkaStateCheck.get.consumersSummaryAction(testClusterName).apply(FakeRequest())
    assert(status(future) === OK)
    val json = Json.parse(contentAsJson(future).toString())
    (json \ "consumers").asOpt[Seq[Map[String, String]]] should not be empty
  }

}
