/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager

import java.util.Properties

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import kafka.manager.utils.CuratorAwareTest
import ActorModel._
import kafka.test.SeededBroker

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Try

/**
 * @author hiral
 */
class TestKafkaManagerActor extends CuratorAwareTest {

  private[this] val akkaConfig: Properties = new Properties()
  akkaConfig.setProperty("pinned-dispatcher.type","PinnedDispatcher")
  akkaConfig.setProperty("pinned-dispatcher.executor","thread-pool-executor")
  private[this] val config : Config = ConfigFactory.parseProperties(akkaConfig)
  private[this] val system = ActorSystem("test-kafka-state-actor",config)
  private[this] val broker = new SeededBroker("km-test",4)
  private[this] val kafkaServerZkPath = broker.getZookeeperConnectionString
  private[this] var kafkaManagerActor : Option[ActorRef] = None
  private[this] implicit val timeout: Timeout = 10.seconds

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val curatorConfig = CuratorConfig(testServer.getConnectString)
    val config = KafkaManagerActorConfig(
      curatorConfig = curatorConfig,
      kafkaManagerUpdatePeriod = FiniteDuration(1,SECONDS),
      deleteClusterUpdatePeriod = FiniteDuration(1,SECONDS)
    )
    val props = Props(classOf[KafkaManagerActor],config)

    kafkaManagerActor = Some(system.actorOf(props,"kafka-manager"))
    Thread.sleep(1000)
  }

  override protected def afterAll(): Unit = {
    kafkaManagerActor.foreach( _ ! KMShutdown)
    system.shutdown()
    Try(broker.shutdown())
    super.afterAll()
  }

  private[this] def withKafkaManagerActor[Input,Output,FOutput](msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output]) : FOutput = {
    require(kafkaManagerActor.isDefined, "kafkaManagerActor undefined!")
    val future = ask(kafkaManagerActor.get, msg).mapTo[Output]
    val result = Await.result(future,10.seconds)
    fn(result)
  }

  test("add cluster") {
    val cc = ClusterConfig("dev","0.8.1.1",testServer.getConnectString)
    withKafkaManagerActor(KMAddCluster(cc)) { result: KMCommandResult =>
      result.result.get
      Thread.sleep(1000)
    }
    withKafkaManagerActor(KMClusterQueryRequest("dev",KSGetTopics)) { result: TopicList =>
      result.list.isEmpty
    }
  }

  test("update cluster zkhost") {
    val cc2 = ClusterConfig("dev","0.8.1.1",kafkaServerZkPath)
    withKafkaManagerActor(KMUpdateCluster(cc2)) { result: KMCommandResult =>
      result.result.get
      Thread.sleep(3000)
    }
    withKafkaManagerActor(KMClusterQueryRequest("dev",KSGetTopics)) { result: TopicList =>
      result.list.nonEmpty
    }
  }

  test("disable cluster") {
    withKafkaManagerActor(KMDisableCluster("dev")) { result: KMCommandResult =>
      result.result.get
      Thread.sleep(1000)
    }
    withKafkaManagerActor(KMClusterQueryRequest("dev",KSGetTopics)) { result: ActorErrorResponse =>
      println(result)
      result.msg.contains("dev")
    }
  }

  test("enable cluster") {
    withKafkaManagerActor(KMEnableCluster("dev")) { result: KMCommandResult =>
      result.result.get
      Thread.sleep(1000)
    }
    withKafkaManagerActor(KMClusterQueryRequest("dev",KSGetTopics)) { result: TopicList =>
      result.list.nonEmpty
    }
  }

  test("update cluster version") {
    val cc2 = ClusterConfig("dev","0.8.2-beta",kafkaServerZkPath)
    withKafkaManagerActor(KMUpdateCluster(cc2)) { result: KMCommandResult =>
      result.result.get
      Thread.sleep(3000)
    }
    withKafkaManagerActor(KMClusterQueryRequest("dev",KSGetTopics)) { result: TopicList =>
      result.list.nonEmpty
    }
  }

  test("delete cluster") {
    withKafkaManagerActor(KMDisableCluster("dev")) { result: KMCommandResult =>
      result.result.get
      Thread.sleep(1000)
    }
    withKafkaManagerActor(KMClusterQueryRequest("dev",KSGetTopics)) { result: ActorErrorResponse =>
      println(result)
      result.msg.contains("dev")
    }
    withKafkaManagerActor(KMDeleteCluster("dev")) { result: KMCommandResult =>
      result.result.get
      Thread.sleep(2000)
    }
    withKafkaManagerActor(KMClusterQueryRequest("dev",KSGetTopics)) { result: ActorErrorResponse =>
      println(result)
      result.msg.contains("dev")
    }
    val cc2 = ClusterConfig("dev","0.8.2-beta",kafkaServerZkPath)
    withKafkaManagerActor(KMAddCluster(cc2)) { result: KMCommandResult =>
      result.result.get
      Thread.sleep(1000)
    }
    withKafkaManagerActor(KMClusterQueryRequest("dev",KSGetTopics)) { result: TopicList =>
      result.list.nonEmpty
    }
  }
}
