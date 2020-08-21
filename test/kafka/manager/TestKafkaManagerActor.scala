/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager

import java.util.Properties
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import kafka.manager.actor.{KafkaManagerActor, KafkaManagerActorConfig}
import kafka.manager.base.LongRunningPoolConfig
import kafka.manager.model.{ActorModel, ClusterConfig, CuratorConfig, SASL_PLAINTEXT}
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
class TestKafkaManagerActor extends CuratorAwareTest with BaseTest {

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
      curatorConfig = curatorConfig
      , kafkaManagerUpdatePeriod = FiniteDuration(1,SECONDS)
      , deleteClusterUpdatePeriod = FiniteDuration(1,SECONDS)
      , defaultTuning = defaultTuning
      , consumerProperties = None
    )
    val props = Props(classOf[KafkaManagerActor],config)

    kafkaManagerActor = Some(system.actorOf(props,"kafka-manager"))
    Thread.sleep(1000)
  }

  override protected def afterAll(): Unit = {
    kafkaManagerActor.foreach( _ ! KMShutdown)
    Try(Await.ready(system.terminate(), Duration(5, TimeUnit.SECONDS)))
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
    val cc = ClusterConfig("dev","2.4.1",testServer.getConnectString, jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(defaultTuning), securityProtocol="PLAINTEXT", saslMechanism = None, jaasConfig = None)
    withKafkaManagerActor(KMAddCluster(cc)) { result: KMCommandResult =>
      result.result.get
      Thread.sleep(1000)
    }
    withKafkaManagerActor(KMClusterQueryRequest("dev",KSGetTopics)) { result: TopicList =>
      result.list.isEmpty
    }
  }

  test("update cluster zkhost") {
    val cc2 = ClusterConfig("dev","2.4.1",kafkaServerZkPath, jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(defaultTuning), securityProtocol="PLAINTEXT", saslMechanism = None, jaasConfig = None)
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
    val cc2 = ClusterConfig("dev","2.4.1",kafkaServerZkPath, jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(defaultTuning), securityProtocol="PLAINTEXT", saslMechanism = None, jaasConfig = None)
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
    val cc2 = ClusterConfig("dev","2.4.1",kafkaServerZkPath, jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(defaultTuning), securityProtocol="PLAINTEXT", saslMechanism = None, jaasConfig = None)
    withKafkaManagerActor(KMAddCluster(cc2)) { result: KMCommandResult =>
      result.result.get
      Thread.sleep(1000)
    }
    withKafkaManagerActor(KMClusterQueryRequest("dev",KSGetTopics)) { result: TopicList =>
      result.list.nonEmpty
    }
  }

  test("get all broker views") {
    withKafkaManagerActor(KMClusterQueryRequest("dev", BVGetViews)) {
      result: Map[Int, BVView] => result.nonEmpty
    }
  }

  test("update cluster logkafka enabled") {
    val cc2 = ClusterConfig("dev","2.4.1",kafkaServerZkPath, jmxEnabled = false, pollConsumers = true, filterConsumers = true, logkafkaEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(defaultTuning), securityProtocol="PLAINTEXT", saslMechanism = None, jaasConfig = None)
    withKafkaManagerActor(KMUpdateCluster(cc2)) { result: KMCommandResult =>
      result.result.get
      Thread.sleep(3000)
    }
    withKafkaManagerActor(KMClusterQueryRequest("dev",LKSGetLogkafkaLogkafkaIds)) { result: LogkafkaLogkafkaIdList =>
      result.list.nonEmpty
    }
  }

  test("update cluster tuning") {
    val newTuning = getClusterTuning(3, 101, 11, 10000, 10000, 1)
    val cc2 = ClusterConfig("dev","2.4.1",kafkaServerZkPath, jmxEnabled = false, pollConsumers = true, filterConsumers = true, logkafkaEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false,
      tuning = Option(newTuning), securityProtocol="PLAINTEXT", saslMechanism = None, jaasConfig = None
    )
    withKafkaManagerActor(KMUpdateCluster(cc2)) { result: KMCommandResult =>
      result.result.get
      Thread.sleep(3000)
    }
    withKafkaManagerActor(KMClusterQueryRequest("dev",KSGetTopics)) { result: TopicList =>
      result.list.isEmpty
    }
    withKafkaManagerActor(KMGetClusterConfig("dev")) { result: KMClusterConfigResult =>
      assert(result.result.isSuccess)
      assert(result.result.toOption.get.tuning.get === newTuning)
    }
  }

  test("update cluster security protocol") {
    val cc2 = ClusterConfig("dev","2.4.1",kafkaServerZkPath, jmxEnabled = false, pollConsumers = true, filterConsumers = true, logkafkaEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(defaultTuning), securityProtocol="SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    withKafkaManagerActor(KMUpdateCluster(cc2)) { result: KMCommandResult =>
      result.result.get
      Thread.sleep(3000)
    }
    withKafkaManagerActor(KMClusterQueryRequest("dev",LKSGetLogkafkaLogkafkaIds)) { result: LogkafkaLogkafkaIdList =>
      result.list.nonEmpty
    }
    withKafkaManagerActor(KMGetClusterConfig("dev")) { result: KMClusterConfigResult =>
      assert(result.result.isSuccess)
      assert(result.result.toOption.get.securityProtocol === SASL_PLAINTEXT)
    }
  }
}
