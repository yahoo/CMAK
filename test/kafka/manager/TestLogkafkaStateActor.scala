/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager

import java.util.Properties

import akka.actor.{ActorRef, ActorSystem, Kill, Props}
import akka.pattern._
import akka.util.Timeout
import akka.util.Timeout._
import com.typesafe.config.{Config, ConfigFactory}
import kafka.manager.features.ClusterFeatures
import kafka.manager.utils.KafkaServerInTest
import ActorModel._
import kafka.test.SeededBroker

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Try

/**
 * @author hiral
 */
class TestLogkafkaStateActor extends KafkaServerInTest {

  private[this] val akkaConfig: Properties = new Properties()
  akkaConfig.setProperty("pinned-dispatcher.type","PinnedDispatcher")
  akkaConfig.setProperty("pinned-dispatcher.executor","thread-pool-executor")
  private[this] val config : Config = ConfigFactory.parseProperties(akkaConfig)
  private[this] val system = ActorSystem("test-logkafka-state-actor",config)
  private[this] val broker = new SeededBroker("ks-test",4)
  override val kafkaServerZkPath = broker.getZookeeperConnectionString
  private[this] var logkafkaStateActor : Option[ActorRef] = None
  private[this] implicit val timeout: Timeout = 10.seconds
  private[this] val defaultClusterConfig = ClusterConfig("test","0.8.2.0","localhost:2818",100,false,true)
  private[this] val defaultClusterContext = ClusterContext(ClusterFeatures.from(defaultClusterConfig), defaultClusterConfig)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val props = Props(classOf[LogkafkaStateActor],sharedCurator, defaultClusterContext)

    logkafkaStateActor = Some(system.actorOf(props.withDispatcher("pinned-dispatcher"),"lksa"))
  }

  override protected def afterAll(): Unit = {
    logkafkaStateActor.foreach( _ ! Kill )
    system.shutdown()
    Try(broker.shutdown())
    super.afterAll()
  }

  private[this] def withLogkafkaStateActor[Input,Output,FOutput](msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output]) : FOutput = {
    require(logkafkaStateActor.isDefined, "logkafkaStateActor undefined!")
    val future = ask(logkafkaStateActor.get, msg).mapTo[Output]
    val result = Await.result(future,10.seconds)
    fn(result)
  }

  test("get logkafka hostname list") {
    withLogkafkaStateActor(LKSGetLogkafkaHostnames) { result: LogkafkaHostnameList =>
      result.list foreach println
    }
  }

  test("get logkafka config") {
    withLogkafkaStateActor(LKSGetLogkafkaHostnames) { result: LogkafkaHostnameList =>
      val configs = result.list map { hostname =>
        withLogkafkaStateActor(LKSGetLogkafkaConfig(hostname)) { logkafkaConfig: LogkafkaConfig => logkafkaConfig }
      }
      configs foreach println
    }
  }

  test("get logkafka client") {
    withLogkafkaStateActor(LKSGetLogkafkaHostnames) { result: LogkafkaHostnameList =>
      val clients = result.list map { hostname =>
        withLogkafkaStateActor(LKSGetLogkafkaClient(hostname)) { logkafkaClient: LogkafkaClient => logkafkaClient }
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
