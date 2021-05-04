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
import kafka.manager.actor.cluster.{KafkaManagedOffsetCacheConfig, KafkaStateActor, KafkaStateActorConfig}
import kafka.manager.base.LongRunningPoolConfig
import kafka.manager.features.ClusterFeatures
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
class TestKafkaStateActor extends KafkaServerInTest with BaseTest {

  private[this] val akkaConfig: Properties = new Properties()
  akkaConfig.setProperty("pinned-dispatcher.type","PinnedDispatcher")
  akkaConfig.setProperty("pinned-dispatcher.executor","thread-pool-executor")
  private[this] val config : Config = ConfigFactory.parseProperties(akkaConfig)
  private[this] val system = ActorSystem("test-kafka-state-actor",config)
  private[this] val broker = new SeededBroker("ks-test",4)
  override val kafkaServerZkPath = broker.getZookeeperConnectionString
  private[this] var kafkaStateActor : Option[ActorRef] = None
  private[this] implicit val timeout: Timeout = 10.seconds
  private[this] val defaultClusterConfig = ClusterConfig("test","0.8.2.0","localhost:2818",100,false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(defaultTuning), securityProtocol="PLAINTEXT", saslMechanism=None, jaasConfig=None)
  private[this] val defaultClusterContext = ClusterContext(ClusterFeatures.from(defaultClusterConfig), defaultClusterConfig)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val ksConfig = KafkaStateActorConfig(
      sharedCurator
      , "pinned-dispatcher"
      , defaultClusterContext
      , LongRunningPoolConfig(2,100)
      , LongRunningPoolConfig(2,100)
      , 5
      , 10000
      , None
      , KafkaManagedOffsetCacheConfig()
    )
    val props = Props(classOf[KafkaStateActor],ksConfig)

    kafkaStateActor = Some(system.actorOf(props.withDispatcher("pinned-dispatcher"),"ksa"))
  }

  override protected def afterAll(): Unit = {
    kafkaStateActor.foreach( _ ! Kill )
    Try(Await.ready(system.terminate(), Duration(5, TimeUnit.SECONDS)))
    Try(broker.shutdown())
    super.afterAll()
  }

  private[this] def withKafkaStateActor[Input,Output,FOutput](msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output]) : FOutput = {
    require(kafkaStateActor.isDefined, "kafkaStateActor undefined!")
    val future = ask(kafkaStateActor.get, msg).mapTo[Output]
    val result = Await.result(future,10.seconds)
    fn(result)
  }

  test("get topic list") {
    withKafkaStateActor(KSGetTopics) { result: TopicList =>
      result.list foreach println
    }
  }

  test("get consumer list") {
    withKafkaStateActor(KSGetConsumers) { result: ConsumerList =>
      result.list foreach println
    }
  }

  test("get topic config") {
    withKafkaStateActor(KSGetTopics) { result: TopicList =>
      val configs = result.list map { topic =>
        withKafkaStateActor(KSGetTopicConfig(topic)) { topicConfig: TopicConfig => topicConfig }
      }
      configs foreach println
    }

  }

  test("get broker list") {
    withKafkaStateActor(KSGetBrokers) { result: BrokerList =>
      result.list foreach println
      val brokerIdentityList : IndexedSeq[BrokerIdentity] = result.list
      brokerIdentityList foreach println
    }
  }

  test("get topic description") {
    withKafkaStateActor(KSGetTopics) { result: TopicList =>
      val descriptions = result.list map { topic =>
        withKafkaStateActor(KSGetTopicDescription(topic)) { optionalDesc: Option[TopicDescription] => optionalDesc }
      }
      descriptions foreach println

      withKafkaStateActor(KSGetBrokers) { brokerList: BrokerList =>
        val topicIdentityList : IndexedSeq[TopicIdentity] = descriptions.flatten.map(td => TopicIdentity.from(brokerList, td, None, None, brokerList.clusterContext, None))
        topicIdentityList foreach println
      }
    }
  }

  test("get consumer description") {
    withKafkaStateActor(KSGetConsumers) { result: ConsumerList =>
      val descriptions = result.list map { consumer =>
        withKafkaStateActor(KSGetConsumerDescription(consumer.name, consumer.consumerType)) { optionalDesc: Option[ConsumerDescription] => optionalDesc }
      }
      descriptions foreach println
    }
  }

  test("get all topic descriptions") {
    withKafkaStateActor(KSGetAllTopicDescriptions()) { td: TopicDescriptions =>
      td.descriptions foreach println
    }
  }

  test("get all consumer descriptions") {
    withKafkaStateActor(KSGetAllConsumerDescriptions()) { cd: ConsumerDescriptions =>
      cd.descriptions foreach println
    }
  }

}
