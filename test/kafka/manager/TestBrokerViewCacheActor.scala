/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager

import java.util.Properties

import akka.actor.{ActorRef, ActorSystem, Kill, Props}
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
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
class TestBrokerViewCacheActor extends KafkaServerInTest {
  private[this] val akkaConfig: Properties = new Properties()
  akkaConfig.setProperty("pinned-dispatcher.type","PinnedDispatcher")
  akkaConfig.setProperty("pinned-dispatcher.executor","thread-pool-executor")
  private[this] val config : Config = ConfigFactory.parseProperties(akkaConfig)
  private[this] val system = ActorSystem("test-broker-view-cache-actor",config)
  private[this] val broker = new SeededBroker("bvc-test",4)
  override val kafkaServerZkPath = broker.getZookeeperConnectionString
  private[this] var kafkaStateActor : Option[ActorRef] = None
  private[this] implicit val timeout: Timeout = 10.seconds

  private[this] var brokerViewCacheActor : Option[ActorRef] = None

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val props = Props(classOf[KafkaStateActor],sharedCurator)

    kafkaStateActor = Some(system.actorOf(props.withDispatcher("pinned-dispatcher"),"ksa"))

    val bvcProps = Props(classOf[BrokerViewCacheActor],kafkaStateActor.get.path, FiniteDuration(10, SECONDS))

    brokerViewCacheActor = Some(system.actorOf(bvcProps,"broker-view"))

    brokerViewCacheActor.get ! BVForceUpdate
    Thread.sleep(10000)
  }

  override protected def afterAll(): Unit = {
    brokerViewCacheActor.foreach( _ ! Kill )
    kafkaStateActor.foreach( _ ! Kill )
    system.shutdown()
    Try(broker.shutdown())
    super.afterAll()
  }

  private[this] def withBrokerViewCacheActor[Input,Output,FOutput]
  (msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output]) : FOutput = {
    require(brokerViewCacheActor.isDefined, "brokerViewCacheActor undefined!")
    val future = ask(brokerViewCacheActor.get, msg).mapTo[Output]
    val result = Await.result(future,10.seconds)
    fn(result)
  }

  test("get broker view") {
    withBrokerViewCacheActor(BVGetView(1)) { optionalBrokerView : Option[BVView] =>
      println(optionalBrokerView)
    }
  }

}
