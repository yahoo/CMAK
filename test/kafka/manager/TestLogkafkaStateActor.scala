/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager

import akka.actor.{ActorRef, ActorSystem, Kill, Props}
import akka.pattern._
import akka.util.Timeout
import akka.util.Timeout._
import com.typesafe.config.{Config, ConfigFactory}
import kafka.manager.features.ClusterFeatures
import kafka.manager.logkafka.LogkafkaStateActor
import kafka.manager.model.ActorModel._
import kafka.manager.model.{ActorModel, ClusterConfig, ClusterContext}
import kafka.manager.utils.KafkaServerInTest
import kafka.test.SeededBroker

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Try

/**
 * @author hiral
 */
class TestLogkafkaStateActor extends KafkaServerInTest with BaseTest {

  private[this] val akkaConfig: Properties = new Properties()
  akkaConfig.setProperty("pinned-dispatcher.type","PinnedDispatcher")
  akkaConfig.setProperty("pinned-dispatcher.executor","thread-pool-executor")
  private[this] val config : Config = ConfigFactory.parseProperties(akkaConfig)
  private[this] val system = ActorSystem("test-logkafka-state-actor",config)
  private[this] val broker = new SeededBroker("ks-test",4)
  override val kafkaServerZkPath = broker.getZookeeperConnectionString
  private[this] var logkafkaStateActor : Option[ActorRef] = None
  private[this] implicit val timeout: Timeout = 10.seconds
  private[this] val defaultClusterConfig = ClusterConfig("test","0.8.2.0","localhost:2818",100,false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(defaultTuning), securityProtocol="PLAINTEXT", saslMechanism=None, jaasConfig=None)
  private[this] val defaultClusterContext = ClusterContext(ClusterFeatures.from(defaultClusterConfig), defaultClusterConfig)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val props = Props(classOf[LogkafkaStateActor],sharedCurator, defaultClusterContext)

    logkafkaStateActor = Some(system.actorOf(props.withDispatcher("pinned-dispatcher"),"lksa"))
  }

  override protected def afterAll(): Unit = {
    logkafkaStateActor.foreach( _ ! Kill )
    Try(Await.ready(system.terminate(), Duration(5, TimeUnit.SECONDS)))
    Try(broker.shutdown())
    super.afterAll()
  }

  private[this] def withLogkafkaStateActor[Input,Output,FOutput](msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output]) : FOutput = {
    require(logkafkaStateActor.isDefined, "logkafkaStateActor undefined!")
    val future = ask(logkafkaStateActor.get, msg).mapTo[Output]
    val result = Await.result(future,10.seconds)
    fn(result)
  }

  test("get logkafka logkafka id list") {
    withLogkafkaStateActor(LKSGetLogkafkaLogkafkaIds) { result: LogkafkaLogkafkaIdList =>
      result.list foreach println
    }
  }

  test("get logkafka config") {
    withLogkafkaStateActor(LKSGetLogkafkaLogkafkaIds) { result: LogkafkaLogkafkaIdList =>
      val configs = result.list map { logkafka_id =>
        withLogkafkaStateActor(LKSGetLogkafkaConfig(logkafka_id)) { logkafkaConfig: LogkafkaConfig => logkafkaConfig }
      }
      configs foreach println
    }
  }

  test("get logkafka client") {
    withLogkafkaStateActor(LKSGetLogkafkaLogkafkaIds) { result: LogkafkaLogkafkaIdList =>
      val clients = result.list map { logkafka_id =>
        withLogkafkaStateActor(LKSGetLogkafkaClient(logkafka_id)) { logkafkaClient: LogkafkaClient => logkafkaClient }
      }
      clients foreach println
    }
  }

  test("get logkafka configs") {
    withLogkafkaStateActor(LKSGetAllLogkafkaConfigs()) { lc: LogkafkaConfigs =>
      lc.configs foreach println
    }
  }

  test("get logkafka clients") {
    withLogkafkaStateActor(LKSGetAllLogkafkaClients()) { lc: LogkafkaClients =>
      lc.clients foreach println
    }
  }

}
