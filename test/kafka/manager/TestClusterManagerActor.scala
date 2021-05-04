/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import kafka.manager.actor.cluster.{ClusterManagerActor, ClusterManagerActorConfig}
import kafka.manager.model.ActorModel._
import kafka.manager.model.{ActorModel, ClusterConfig, CuratorConfig}
import kafka.manager.utils.zero81.PreferredLeaderElectionErrors
import kafka.manager.utils.{CuratorAwareTest, ZkUtils}
import kafka.test.SeededBroker

import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.Try

/**
 * @author hiral
 */
class TestClusterManagerActor extends CuratorAwareTest with BaseTest {

  private[this] val akkaConfig: Properties = new Properties()
  akkaConfig.setProperty("pinned-dispatcher.type","PinnedDispatcher")
  akkaConfig.setProperty("pinned-dispatcher.executor","thread-pool-executor")
  private[this] val config : Config = ConfigFactory.parseProperties(akkaConfig)
  private[this] val system = ActorSystem("test-kafka-state-actor",config)
  private[this] val broker = new SeededBroker("cm-test",4)
  private[this] val kafkaServerZkPath = broker.getZookeeperConnectionString
  private[this] var clusterManagerActor : Option[ActorRef] = None
  private[this] implicit val timeout: Timeout = 10.seconds
  private[this] val createAndDeleteTopicName = "cm-cd-unit-test"
  private[this] val createTopicName = "cm-unit-test"
  private[this] val createLogkafkaLogkafkaId = "km-unit-test-logkafka-logkafka_id"
  private[this] val createLogkafkaLogPath = "/km-unit-test-logkafka-logpath"
  private[this] val createLogkafkaTopic = "km-unit-test-logkafka-topic"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val clusterConfig = ClusterConfig("dev","0.8.2.0",kafkaServerZkPath, jmxEnabled = false, pollConsumers = true, filterConsumers = true, logkafkaEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(defaultTuning), securityProtocol="PLAINTEXT", saslMechanism=None, jaasConfig=None)
    val curatorConfig = CuratorConfig(testServer.getConnectString)
    val config = ClusterManagerActorConfig(
      "pinned-dispatcher"
      ,"/kafka-manager/clusters/dev"
      ,curatorConfig,clusterConfig
      ,None
    )
    val props = Props(classOf[ClusterManagerActor],config)

    clusterManagerActor = Some(system.actorOf(props,"dev"))
  }

  override protected def afterAll(): Unit = {
    Thread.sleep(1000)
    Try(clusterManagerActor.foreach( _ ! CMShutdown))
    Try(Await.ready(system.terminate(), Duration(5, TimeUnit.SECONDS)))
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
      assert(result.list.contains(createTopicName),s"Failed to create topic : $createTopicName")
    }
    withClusterManagerActor(CMCreateTopic(createAndDeleteTopicName,4,1)) { cmResultFuture: Future[CMCommandResult] =>
      val cmResult = Await.result(cmResultFuture,10 seconds)
      if(cmResult.result.isFailure) {
        cmResult.result.get
      }
      Thread.sleep(500)
    }
    withClusterManagerActor(KSGetTopics) { result: TopicList =>
      assert(result.list.contains(createAndDeleteTopicName),s"Failed to create topic : $createAndDeleteTopicName")
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
        val topicIdentityList : IndexedSeq[TopicIdentity] = descriptions.flatten.map(td => TopicIdentity.from(brokerList, td, None, None, brokerList.clusterContext, None))
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
      val brokers = Set(0)
      withClusterManagerActor(CMGeneratePartitionAssignments(topicSet, brokers)) { cmResults: CMCommandResults =>
        cmResults.result.foreach { t =>
          if(t.isFailure) {
            t.get
          }
        }
      }
      Thread.sleep(1000)
      withCurator { curator =>
        topicSet.foreach { topic =>
          val data =  curator.getData.forPath(s"/kafka-manager/clusters/dev/topics/$topic")
          assert(data != null)
          println(s"$topic -> " + ClusterManagerActor.deserializeAssignments(data))
        }
      }
    }
  }

  test("get partition assignments for topic") {
    withClusterManagerActor(KSGetTopics) { result : TopicList =>
      result.list.foreach { topic =>
        val brokers = Set(0)
        withClusterManagerActor(CMGetGeneratedPartitionAssignments(topic)) { gpa: GeneratedPartitionAssignments =>
          assert(gpa.assignments.nonEmpty)
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
      withClusterManagerActor(CMRunReassignPartition(topicSet, Set.empty)) { cmResultsFuture: Future[CMCommandResults] =>
        val cmResult = Await.result(cmResultsFuture,10 seconds)
        Thread.sleep(1000)
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
      assert(result.list.contains(createAndDeleteTopicName),"Cannot delete topic which doesn't exist")
    }
    withClusterManagerActor(CMDeleteTopic(createAndDeleteTopicName)) { cmResultFuture: Future[CMCommandResult] =>
      val cmResult = Await.result(cmResultFuture,10 seconds)
      if(cmResult.result.isFailure) {
        cmResult.result.get
      }
    }
    Thread.sleep(3000)
    withClusterManagerActor(KSGetTopics) { result: TopicList =>
      assert((!result.list.contains(createAndDeleteTopicName)) ||
        result.deleteSet(createAndDeleteTopicName),s"Failed to delete topic : $result")
    }
  }

  test("create logkafka") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp,createLogkafkaTopic)
    withClusterManagerActor(CMCreateLogkafka(createLogkafkaLogkafkaId,createLogkafkaLogPath,config)) { cmResultFuture: Future[CMCommandResult] =>
      val cmResult = Await.result(cmResultFuture,10 seconds)
      if(cmResult.result.isFailure) {
        cmResult.result.get
      }
      Thread.sleep(500)
    }

    withClusterManagerActor(LKSGetLogkafkaLogkafkaIds) { result: LogkafkaLogkafkaIdList =>
      assert(result.list.contains(createLogkafkaLogkafkaId),"Failed to create logkafka")
    }

    withClusterManagerActor(CMGetLogkafkaIdentity(createLogkafkaLogkafkaId)) { result: Option[CMLogkafkaIdentity] =>
      assert(result.get.logkafkaIdentity.get.identityMap.contains(createLogkafkaLogPath),"Failed to create logkafka")
    }
  }

  test("get logkafka logkafka id list") {
    withClusterManagerActor(LKSGetLogkafkaLogkafkaIds) { result: LogkafkaLogkafkaIdList =>
      assert(result.list.nonEmpty,"Failed to get logkafka logkafka_id list")
      result.list foreach println
    }
  }

  test("get logkafka config") {
    withClusterManagerActor(LKSGetLogkafkaLogkafkaIds) { result: LogkafkaLogkafkaIdList =>
      val configs = result.list map { logkafka_id =>
        withClusterManagerActor(LKSGetLogkafkaConfig(logkafka_id)) { logkafkaConfigOption: Option[LogkafkaConfig] => logkafkaConfigOption.get }
      }
      configs foreach println
    }
  }

  test("delete logkafka") {
    withClusterManagerActor(CMDeleteLogkafka(createLogkafkaLogkafkaId,createLogkafkaLogPath)) { cmResultFuture: Future[CMCommandResult] =>
      val cmResult = Await.result(cmResultFuture,10 seconds)
      if(cmResult.result.isFailure) {
        cmResult.result.get
      }
      Thread.sleep(500)
    }

    withClusterManagerActor(CMGetLogkafkaIdentity(createLogkafkaLogkafkaId)) { result: Option[CMLogkafkaIdentity] =>
      assert(!result.get.logkafkaIdentity.get.identityMap.contains(createLogkafkaLogPath),"Failed to delete logkafka")
    }
  }
}
