/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager

import java.nio.charset.StandardCharsets
import java.util.Properties

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import kafka.manager.utils.zero81.PreferredLeaderElectionErrors
import kafka.test.SeededBroker
import kafka.manager.utils.{CuratorAwareTest, ZkUtils}
import ActorModel._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.Try

/**
 * @author hiral
 */
class TestClusterManagerActor extends CuratorAwareTest {

  private[this] val akkaConfig: Properties = new Properties()
  akkaConfig.setProperty("pinned-dispatcher.type","PinnedDispatcher")
  akkaConfig.setProperty("pinned-dispatcher.executor","thread-pool-executor")
  private[this] val config : Config = ConfigFactory.parseProperties(akkaConfig)
  private[this] val system = ActorSystem("test-kafka-state-actor",config)
  private[this] val broker = new SeededBroker("cm-test",4)
  private[this] val kafkaServerZkPath = broker.getZookeeperConnectionString
  private[this] var clusterManagerActor : Option[ActorRef] = None
  private[this] implicit val timeout: Timeout = 10.seconds
  private[this] val createTopicName = "cm-unit-test"
  private[this] val createLogkafkaHostname = "km-unit-test-logkafka-hostname"
  private[this] val createLogkafkaLogPath = "/km-unit-test-logkafka-logpath"
  private[this] val createLogkafkaTopic = "km-unit-test-logkafka-topic"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val clusterConfig = ClusterConfig("dev","0.8.2.0",kafkaServerZkPath, jmxEnabled = false, filterConsumers = true, logkafkaEnabled = true)
    val curatorConfig = CuratorConfig(testServer.getConnectString)
    val config = ClusterManagerActorConfig("pinned-dispatcher","/kafka-manager/clusters/dev",curatorConfig,clusterConfig,FiniteDuration(1,SECONDS))
    val props = Props(classOf[ClusterManagerActor],config)

    clusterManagerActor = Some(system.actorOf(props,"dev"))
  }

  override protected def afterAll(): Unit = {
    Thread.sleep(1000)
    Try(clusterManagerActor.foreach( _ ! CMShutdown))
    Try(system.shutdown())
    Try(broker.shutdown())
    super.afterAll()
  }

  private[this] def withClusterManagerActor[Input,Output,FOutput](msg: Input)(fn: Output => FOutput)(implicit tag: ClassTag[Output]) : FOutput = {
    require(clusterManagerActor.isDefined, "clusterManagerActor undefined!")
    val future = ask(clusterManagerActor.get, msg).mapTo[Output]
    val result = Await.result(future,10.seconds)
    fn(result)
  }

  test("create topic") {
    withClusterManagerActor(CMCreateTopic(createTopicName,4,1)) { cmResultFuture: Future[CMCommandResult] =>
      val cmResult = Await.result(cmResultFuture,10 seconds)
      if(cmResult.result.isFailure) {
        cmResult.result.get
      }
      Thread.sleep(500)
    }
    withClusterManagerActor(KSGetTopics) { result: TopicList =>
      assert(result.list.contains(createTopicName),"Failed to create topic")
    }
  }

  test("fail to create topic again") {
    withClusterManagerActor(CMCreateTopic(createTopicName,4,1)) { cmResultFuture: Future[CMCommandResult] =>
      val cmResult = Await.result(cmResultFuture,10 seconds)
      assert(cmResult.result.isFailure, "We created the same topic twice!")
    }
  }

  test("get topic list") {
    withClusterManagerActor(KSGetTopics) { result: TopicList =>
      assert(result.list.nonEmpty,"Failed to get topic list!")
      result.list foreach println
    }
  }

  test("get topic config") {
    withClusterManagerActor(KSGetTopics) { result: TopicList =>
      val configs = result.list map { topic =>
        withClusterManagerActor(KSGetTopicConfig(topic)) { topicConfig: TopicConfig => topicConfig }
      }
      configs foreach println
    }

  }

  test("get broker list") {
    withClusterManagerActor(KSGetBrokers) { result: BrokerList =>
      result.list foreach println
      val brokerIdentityList : IndexedSeq[BrokerIdentity] = result.list
      brokerIdentityList foreach println
    }
  }

  test("get topic description") {
    withClusterManagerActor(KSGetTopics) { result: TopicList =>
      val descriptions = result.list map { topic =>
        withClusterManagerActor(KSGetTopicDescription(topic)) { optionalDesc: Option[TopicDescription] => optionalDesc }
      }
      descriptions foreach println

      withClusterManagerActor(KSGetBrokers) { brokerList: BrokerList =>
        val topicIdentityList : IndexedSeq[TopicIdentity] = descriptions.flatten.map(td => TopicIdentity.from(brokerList,td, None, brokerList.clusterContext, None))
        topicIdentityList foreach println
      }
    }
  }

  test("get topic descriptions") {
    withClusterManagerActor(KSGetAllTopicDescriptions()) { td: TopicDescriptions =>
      td.descriptions foreach println
    }
  }

  test("get broker view") {
    println("Waiting for broker view update...")
    Thread.sleep(2000)
    println("Querying broker view...")
    withClusterManagerActor(BVGetView(0)) { optionalBrokerView : Option[BVView] =>
      println(optionalBrokerView)
    }
  }

  test("generate partition assignments for topic") {
    withClusterManagerActor(KSGetTopics) { result : TopicList =>
      val topicSet = result.list.toSet
      val brokers = Seq(0)
      withClusterManagerActor(CMGeneratePartitionAssignments(topicSet, brokers)) { cmResults: CMCommandResults =>
        cmResults.result.foreach { t =>
          if(t.isFailure) {
            t.get
          }
        }
      }
      Thread.sleep(2000)
      withCurator { curator =>
        topicSet.foreach { topic =>
          val data =  curator.getData.forPath(s"/kafka-manager/clusters/dev/topics/$topic")
          assert(data != null)
          println(s"$topic -> " + ClusterManagerActor.deserializeAssignments(data))
        }
      }
    }
  }

  test("manual partition assignments for topic") {
    val assignment = List(
      ("cm-test",List(
        (0, List(0)), (1, List(0)), (2, List(0)), (3, List(0)))
      ),
      ("cm-unit-test",List(
        (0, List(0)), (1, List(0)), (2, List(0)), (3, List(0)))
      )
    )

    withClusterManagerActor(CMManualPartitionAssignments(assignment)) { cmResults: CMCommandResults =>
      cmResults.result.foreach { t =>
        if (t.isFailure) {
          t.get
        }
      }
    }
    Thread.sleep(2000)
    withCurator { curator =>
      val topics = for {
        (topic, topicAssignment) <- assignment
      } yield {
        topic
      }

      topics.foreach { topic =>
        val data =  curator.getData.forPath(s"/kafka-manager/clusters/dev/topics/$topic")
        assert(data != null)
        println(s"$topic -> " + ClusterManagerActor.deserializeAssignments(data))
      }
    }
  }

  test("run preferred leader election for topic") {
    withClusterManagerActor(KSGetTopics) { result : TopicList =>
      val topicSet = result.list.toSet
      withClusterManagerActor(CMRunPreferredLeaderElection(topicSet)) { cmResultFuture: Future[CMCommandResult] =>
        val cmResult = Await.result(cmResultFuture,10 seconds)
        if (cmResult.result.isFailure) {
          checkError[PreferredLeaderElectionErrors.ElectionSetEmptyOnWrite] {
            cmResult.result.get
          }
        } else {
          withCurator { curator =>
            val data = curator.getData.forPath(ZkUtils.PreferredReplicaLeaderElectionPath)
            assert(data != null)
            println(new String(data, StandardCharsets.UTF_8))
          }
        }
      }
    }
  }

  test("run reassign partition for topic") {
    withClusterManagerActor(KSGetTopics) { result : TopicList =>
      val topicSet = result.list.toSet
      withClusterManagerActor(CMRunReassignPartition(topicSet)) { cmResultsFuture: Future[CMCommandResults] =>
        val cmResult = Await.result(cmResultsFuture,10 seconds)
        cmResult.result.foreach { t =>
          if(t.isFailure) {
            t.get
          }
        }
      }
    }
  }

  test("delete topic") {
    withClusterManagerActor(KSGetTopics) { result: TopicList =>
      assert(result.list.contains(createTopicName),"Cannot delete topic which doesn't exist")
    }
    withClusterManagerActor(CMDeleteTopic(createTopicName)) { cmResultFuture: Future[CMCommandResult] =>
      val cmResult = Await.result(cmResultFuture,10 seconds)
      if(cmResult.result.isFailure) {
        cmResult.result.get
      }
    }
    Thread.sleep(3000)
    withClusterManagerActor(KSGetTopics) { result: TopicList =>
      assert(!result.list.contains(createTopicName),"Failed to delete topic")
    }
  }

  test("create logkafka") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp,createLogkafkaTopic)
    withClusterManagerActor(CMCreateLogkafka(createLogkafkaHostname,createLogkafkaLogPath,config)) { cmResultFuture: Future[CMCommandResult] =>
      val cmResult = Await.result(cmResultFuture,10 seconds)
      if(cmResult.result.isFailure) {
        cmResult.result.get
      }
      Thread.sleep(500)
    }

    withClusterManagerActor(LKSGetLogkafkaHostnames) { result: LogkafkaHostnameList =>
      assert(result.list.contains(createLogkafkaHostname),"Failed to create logkafka")
    }

    withClusterManagerActor(CMGetLogkafkaIdentity(createLogkafkaHostname)) { result: Option[CMLogkafkaIdentity] =>
      assert(result.get.logkafkaIdentity.get.identityMap.contains(createLogkafkaLogPath),"Failed to create logkafka")
    }
  }

  test("get logkafka hostname list") {
    withClusterManagerActor(LKSGetLogkafkaHostnames) { result: LogkafkaHostnameList =>
      assert(result.list.nonEmpty,"Failed to get logkafka hostname list")
      result.list foreach println
    }
  }

  test("get logkafka config") {
    withClusterManagerActor(LKSGetLogkafkaHostnames) { result: LogkafkaHostnameList =>
      val configs = result.list map { hostname =>
        withClusterManagerActor(LKSGetLogkafkaConfig(hostname)) { logkafkaConfigOption: Option[LogkafkaConfig] => logkafkaConfigOption.get }
      }
      configs foreach println
    }
  }

  test("delete logkafka") {
    withClusterManagerActor(CMDeleteLogkafka(createLogkafkaHostname,createLogkafkaLogPath)) { cmResultFuture: Future[CMCommandResult] =>
      val cmResult = Await.result(cmResultFuture,10 seconds)
      if(cmResult.result.isFailure) {
        cmResult.result.get
      }
      Thread.sleep(500)
    }

    withClusterManagerActor(CMGetLogkafkaIdentity(createLogkafkaHostname)) { result: Option[CMLogkafkaIdentity] =>
      assert(!result.get.logkafkaIdentity.get.identityMap.contains(createLogkafkaLogPath),"Failed to delete logkafka")
    }
  }
}
