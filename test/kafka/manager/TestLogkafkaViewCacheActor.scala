/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager

import akka.actor.{ActorRef, ActorSystem, Kill, Props}
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import kafka.manager.actor.cluster.KafkaStateActor
import kafka.manager.base.LongRunningPoolConfig
import kafka.manager.features.ClusterFeatures
import kafka.manager.logkafka.{LogkafkaViewCacheActor, LogkafkaViewCacheActorConfig}
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
class TestLogkafkaViewCacheActor extends KafkaServerInTest with BaseTest {
  private[this] val akkaConfig: Properties = new Properties()
  akkaConfig.setProperty("pinned-dispatcher.type","PinnedDispatcher")
  akkaConfig.setProperty("pinned-dispatcher.executor","thread-pool-executor")
  private[this] val config : Config = ConfigFactory.parseProperties(akkaConfig)
  private[this] val system = ActorSystem("test-logkafka-view-cache-actor",config)
  private[this] val broker = new SeededBroker("lkvc-test",4)
  override val kafkaServerZkPath = broker.getZookeeperConnectionString
  private[this] var logkafkaStateActor : Option[ActorRef] = None
  private[this] implicit val timeout: Timeout = 10.seconds

  private[this] var logkafkaViewCacheActor : Option[ActorRef] = None
  private[this] val defaultClusterConfig = ClusterConfig("test","0.8.2.0","localhost:2818",100,false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(defaultTuning), securityProtocol="PLAINTEXT", saslMechanism=None, jaasConfig=None)
  private[this] val defaultClusterContext = ClusterContext(ClusterFeatures.from(defaultClusterConfig), defaultClusterConfig)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val clusterConfig = ClusterConfig("dev","0.8.2.0",kafkaServerZkPath, jmxEnabled = false, pollConsumers = true, filterConsumers = true, logkafkaEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(defaultTuning), securityProtocol="PLAINTEXT", saslMechanism=None, jaasConfig=None)
    val clusterContext = ClusterContext(ClusterFeatures.from(clusterConfig), clusterConfig)
    val props = Props(classOf[KafkaStateActor],sharedCurator, defaultClusterContext)

    logkafkaStateActor = Some(system.actorOf(props.withDispatcher("pinned-dispatcher"),"lksa"))

    val lkvConfig = LogkafkaViewCacheActorConfig(logkafkaStateActor.get.path, clusterContext, LongRunningPoolConfig(2,100), FiniteDuration(10, SECONDS))
    val lkvcProps = Props(classOf[LogkafkaViewCacheActor],lkvConfig)

    logkafkaViewCacheActor = Some(system.actorOf(lkvcProps,"logkafka-view"))

    logkafkaViewCacheActor.get ! BVForceUpdate
    Thread.sleep(10000)
  }

  override protected def afterAll(): Unit = {
    logkafkaViewCacheActor.foreach( _ ! Kill )
    logkafkaStateActor.foreach( _ ! Kill )
    Try(Await.ready(system.terminate(), Duration(5, TimeUnit.SECONDS)))
    Try(broker.shutdown())
    super.afterAll()
  }

  private[this] def withLogkafkaViewCacheActor[Input,Output,FOutput]
  (msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output]) : FOutput = {
    require(logkafkaViewCacheActor.isDefined, "logkafkaViewCacheActor undefined!")
    val future = ask(logkafkaViewCacheActor.get, msg).mapTo[Output]
    val result = Await.result(future,10.seconds)
    fn(result)
  }
}
