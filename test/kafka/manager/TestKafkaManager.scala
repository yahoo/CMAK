/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.Cancellable
import com.typesafe.config.{Config, ConfigFactory}
import kafka.manager.features.KMDeleteTopicFeature
import kafka.manager.model._
import kafka.manager.utils.CuratorAwareTest
import kafka.manager.model.ActorModel.{KafkaManagedConsumer, TopicList, ZKManagedConsumer}
import kafka.test.{HighLevelConsumer, NewKafkaManagedConsumer, SeededBroker, SimpleProducer}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/**
 * @author hiral
 */
class TestKafkaManager extends CuratorAwareTest with BaseTest {
  private[this] val seededTopic = "km-api-test"
  private[this] val broker = new SeededBroker(seededTopic,4)
  private[this] val kafkaServerZkPath = broker.getZookeeperConnectionString
  private[this] val akkaConfig: Properties = new Properties()
  akkaConfig.setProperty("pinned-dispatcher.type","PinnedDispatcher")
  akkaConfig.setProperty("pinned-dispatcher.executor","thread-pool-executor")
  akkaConfig.setProperty("cmak.zkhosts",testServer.getConnectString)
  akkaConfig.setProperty("cmak.broker-view-update-seconds","1")
  akkaConfig.setProperty("cmak.kafka-manager-update-seconds","1")
  akkaConfig.setProperty("cmak.delete-cluster-update-seconds","1")
  akkaConfig.setProperty("cmak.consumer.properties.file","conf/consumer.properties")
  private[this] val config : Config = ConfigFactory.parseProperties(akkaConfig)

  private[this] val kafkaManager : KafkaManager = new KafkaManager(config)

  private[this] val duration = FiniteDuration(10,SECONDS)
  private[this] val createTopicNameA = "km-unit-test-a"
  private[this] val createTopicNameB = "km-unit-test-b"
  private[this] val createLogkafkaLogkafkaId = "km-unit-test-logkafka-logkafka_id"
  private[this] val createLogkafkaLogPath = "/km-unit-test-logkafka-logpath"
  private[this] val createLogkafkaTopic = "km-unit-test-logkafka-topic"
  private[this] var hlConsumer : Option[HighLevelConsumer] = None
  private[this] var hlConsumerThread : Option[Thread] = None
  private[this] val hlShutdown = new AtomicBoolean(false)
  private[this] var newConsumer : Option[NewKafkaManagedConsumer] = None
  private[this] var newConsumerThread : Option[Thread] = None
  private[this] val newShutdown = new AtomicBoolean(false)
  private[this] var simpleProducer : Option[SimpleProducer] = None
  private[this] var simpleProducerThread : Option[Thread] = None

  override protected def beforeAll() : Unit = {
    super.beforeAll()
    hlConsumer = Option(broker.getHighLevelConsumer)
    hlConsumerThread = Option(new Thread() {
      override def run(): Unit = {
        while(!hlShutdown.get()) {
          hlConsumer.map(_.read { ba => {
            println(s"Read ba: $ba")
            Option(ba).map(asString).foreach( s => println(s"hl consumer read message : $s"))
          }
          })
          Thread.sleep(500)
        }
      }
    })
    hlConsumerThread.foreach(_.start())
    newConsumer = Option(broker.getNewConsumer)
    newConsumerThread = Option(new Thread() {
      override def run(): Unit = {
        while(!newShutdown.get()) {
          newConsumer.map(_.read { message =>
            Option(message).foreach( s => println(s"new consumer read message : $s"))
          })
          Thread.sleep(500)
        }
      }
    })
    newConsumerThread.foreach(_.start())
    simpleProducer = Option(broker.getSimpleProducer)
    simpleProducerThread = Option(new Thread() {
      override def run(): Unit = {
        var count = 0
        while(!hlShutdown.get()) {
          simpleProducer.foreach { p =>
            p.send(s"simple message $count", null)
            count+=1
            Thread.sleep(500)
          }
        }
      }
    })
    simpleProducerThread.foreach(_.start())
    Thread.sleep(1000)

    //val future = kafkaManager.addCluster("dev","1.1.0",kafkaServerZkPath, jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(kafkaManager.defaultTuning), securityProtocol = "PLAINTEXT", saslMechanism = "PLAIN", jaasConfig = None)
    //val result = Await.result(future,duration)
    //assert(result.isRight === true)
    //Thread.sleep(2000)
  }

  override protected def afterAll(): Unit = {
    Try(newShutdown.set(true))
    Try(hlShutdown.set(true))
    Try(simpleProducerThread.foreach(_.interrupt()))
    Try(hlConsumerThread.foreach(_.interrupt()))
    Try(hlConsumer.foreach(_.close()))
    Try(newConsumerThread.foreach(_.interrupt()))
    Try(newConsumer.foreach(_.close()))
    if(kafkaManager!=null) {
      kafkaManager.shutdown()
    }
    Try(broker.shutdown())
    super.afterAll()
  }

  private[this] def getTopicList() : TopicList = {
    val future = kafkaManager.getTopicList("dev")
    val result = Await.result(future,duration)
    result.toOption.get
  }

  test("add cluster") {
    val future = kafkaManager.addCluster("dev","2.4.1",kafkaServerZkPath, jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(kafkaManager.defaultTuning), securityProtocol="PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    Thread.sleep(2000)
  }

  test("create topic") {
    val futureA = kafkaManager.createTopic("dev",createTopicNameA,4,1)
    val resultA = Await.result(futureA,duration)
    val futureB = kafkaManager.createTopic("dev",createTopicNameB,4,1)
    val resultB = Await.result(futureB,duration)
    assert(resultA.isRight === true)
    assert(resultB.isRight === true)
    Thread.sleep(2000)
  }

  test("fail to create topic again") {
    val future = kafkaManager.createTopic("dev",createTopicNameA,4,1)
    val result = Await.result(future,duration)
    assert(result.isLeft === true)
    Thread.sleep(2000)
  }

  test("get topic list") {
    val future = kafkaManager.getTopicList("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    assert(result.toOption.get.list.nonEmpty === true)
  }

  test("query request for invalid cluster") {
    val future = kafkaManager.getTopicList("blah")
    val result = Await.result(future,duration)
    assert(result.isLeft === true)
    assert(result.swap.toOption.get.msg.contains("blah") === true)
  }

  test("get broker list") {
    val future = kafkaManager.getBrokerList("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    assert(result.toOption.nonEmpty === true)
  }

  test("get topic identity") {
    val future = kafkaManager.getTopicList("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    assert(result.toOption.get.list.nonEmpty === true)
    result.toOption.get.list.foreach { topic =>
      val future2 = kafkaManager.getTopicIdentity("dev",topic)
      val result2 = Await.result(future2, duration)
      assert(result2.isRight === true)
    }
    //seeded topic should have offsets
    val future2 = kafkaManager.getTopicIdentity("dev",seededTopic)
    val result2 = Await.result(future2, duration)
    assert(result2.isRight === true)
    assert(result2.toOption.get.summedTopicOffsets >= 0)
  }

  test("get cluster list") {
    val future = kafkaManager.getClusterList
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    assert(result.toOption.get.active.nonEmpty === true)
  }

  test("get cluster view") {
    val future = kafkaManager.getClusterView("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
  }

  test("get cluster config") {
    val future = kafkaManager.getClusterConfig("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
  }
  
  test("get cluster context") {
    val future = kafkaManager.getClusterContext("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true, s"Failed : ${result}")
    assert(result.toOption.get.clusterFeatures.features(KMDeleteTopicFeature))
  }
  
  test("get consumer list passive mode") {
    //Thread.sleep(2000)
    val future = kafkaManager.getConsumerListExtended("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true, s"Failed : ${result}")
    assert(result.toOption.get.clusterContext.config.activeOffsetCacheEnabled === false, s"Failed : ${result}")
    assert(result.toOption.get.list.map(_._1).contains((newConsumer.get.groupId, KafkaManagedConsumer)), s"Failed : ${result}")
    //TODO: fix high level consumer test
    //assert(result.toOption.get.list.map(_._1).contains((hlConsumer.get.groupId, KafkaManagedConsumer)), s"Failed : ${result}")
  }

  /*test("get consumer identity passive mode for old consumer") {
    val future = kafkaManager.getConsumerIdentity("dev", hlConsumer.get.groupId, "ZK")
    val result = Await.result(future,duration)
    assert(result.isRight === true, s"Failed : ${result}")
    assert(result.toOption.get.clusterContext.config.activeOffsetCacheEnabled === false, s"Failed : ${result}")
    assert(result.toOption.get.topicMap.head._1 === seededTopic, s"Failed : ${result}")
  }*/

  test("get consumer identity passive mode for new consumer") {
    val future = kafkaManager.getConsumerIdentity("dev", newConsumer.get.groupId, "KF")
    val result = Await.result(future,duration)
    assert(result.isRight === true, s"Failed : ${result}")
    assert(result.toOption.get.clusterContext.config.activeOffsetCacheEnabled === false, s"Failed : ${result}")
    assert(result.toOption.get.topicMap.head._1 === seededTopic, s"Failed : ${result}")
  }

  test("run preferred leader election") {
    val topicList = getTopicList()
    val future = kafkaManager.runPreferredLeaderElection("dev",topicList.list.toSet)
    val result = Await.result(future,duration)
    //TODO: this is a failure since there is nothing to do, need a better test
    assert(result.isLeft === true)
    Thread.sleep(2000)
  }

  test("get preferred leader election") {
    val future = kafkaManager.getPreferredLeaderElection("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    println(result.toOption.get)
  }

  test("schedule preferred leader election") {
    val topicList = getTopicList()
    kafkaManager.schedulePreferredLeaderElection("dev",topicList.list.toSet, 1)
    assert(
      kafkaManager.pleCancellable.contains("dev"),
      "Scheduler not being persisted against the cluster name in KafkaManager instance. Is the task even getting scheduled?"
    )
    assert(
      kafkaManager.pleCancellable("dev")._1.isInstanceOf[Option[Cancellable]],
      "Some(system.scheduler.schedule) instance not being stored in KafkaManager instance. This is required for cancelling."
    )
  }

  test("cancel scheduled preferred leader election") {
    // For cancelling it is necessary for the task to be scheduled
    if(!(kafkaManager.pleCancellable.contains("dev") && kafkaManager.pleCancellable("dev")._1.isInstanceOf[Option[Cancellable]])){
      kafkaManager.schedulePreferredLeaderElection("dev",getTopicList().list.toSet, 1)
    }
    kafkaManager.cancelPreferredLeaderElection("dev")
    assert(
      !kafkaManager.pleCancellable.contains("dev"),
      "Scheduler cluster name is not being removed from KafkaManager instance. Is the task even getting cancelled?"
    )
  }

  test("generate partition assignments") {
    val topicList = getTopicList()
    val future = kafkaManager.generatePartitionAssignments("dev",topicList.list.toSet,Set(0))
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    Thread.sleep(2000)
  }

  test("generate partition assignments with replication factor") {
    val topicList = getTopicList()
    val future = kafkaManager.generatePartitionAssignments("dev", topicList.list.toSet, Set(0), Some(1))
    val result = Await.result(future, duration)
    assert(result.isRight === true)
    Thread.sleep(2000)
  }

  test("fail to generate partition assignments with replication factor larger than available brokers") {
    val topicList = getTopicList()
    val future = kafkaManager.generatePartitionAssignments("dev", topicList.list.toSet, Set(0), Some(2))
    val result = Await.result(future, duration)
    assert(result.isLeft === true)
    Thread.sleep(2000)
  }

  test("run reassign partitions") {
    val topicList = getTopicList()
    val future = kafkaManager.runReassignPartitions("dev",topicList.list.toSet)
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    Thread.sleep(2000)
  }

  test("get reassign partitions") {
    val future = kafkaManager.getReassignPartitions("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
  }

  test("add topic partitions") {
    val tiFuture= kafkaManager.getTopicIdentity("dev",createTopicNameA)
    val tiOrError = Await.result(tiFuture, duration)
    assert(tiOrError.isRight, "Failed to get topic identity!")
    val ti = tiOrError.toOption.get
    val future = kafkaManager.addTopicPartitions("dev",createTopicNameA,Seq(0),ti.partitions + 1,ti.readVersion)
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    Thread.sleep(2000)

    //check new partition num
    {
      val tiFuture= kafkaManager.getTopicIdentity("dev",createTopicNameA)
      val tiOrError = Await.result(tiFuture, duration)
      assert(tiOrError.isRight, "Failed to get topic identity!")
      val ti = tiOrError.toOption.get
      assert(ti.partitions === 5)
    }
  }

  test("add multiple topics partitions") {
    val tiFutureA = kafkaManager.getTopicIdentity("dev",createTopicNameA)
    val tiFutureB = kafkaManager.getTopicIdentity("dev",createTopicNameB)
    val tiOrErrorA = Await.result(tiFutureA,duration)
    val tiOrErrorB = Await.result(tiFutureB,duration)
    assert(tiOrErrorA.isRight, "Failed to get topic identity for topic A!")
    assert(tiOrErrorB.isRight, "Failed to get topic identity for topic B!")
    val tiA = tiOrErrorA.toOption.get
    val tiB = tiOrErrorB.toOption.get
    val newPartitionNum = tiA.partitions + 1
    val future = kafkaManager.addMultipleTopicsPartitions("dev",Seq(createTopicNameA, createTopicNameB),Set(0),newPartitionNum,Map(createTopicNameA->tiA.readVersion,createTopicNameB->tiB.readVersion))
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    Thread.sleep(2000)

    {
      val tiFutureA = kafkaManager.getTopicIdentity("dev",createTopicNameA)
      val tiFutureB = kafkaManager.getTopicIdentity("dev",createTopicNameB)
      val tiOrErrorA = Await.result(tiFutureA,duration)
      val tiOrErrorB = Await.result(tiFutureB,duration)
      assert(tiOrErrorA.isRight, "Failed to get topic identity for topic A!")
      assert(tiOrErrorB.isRight, "Failed to get topic identity for topic B!")
      val tiA = tiOrErrorA.toOption.get
      val tiB = tiOrErrorB.toOption.get
      assert(tiA.partitions === newPartitionNum)
      assert(tiB.partitions === newPartitionNum)
    }
  }

  test("update topic config") {
    val tiFuture= kafkaManager.getTopicIdentity("dev",createTopicNameA)
    val tiOrError = Await.result(tiFuture, duration)
    assert(tiOrError.isRight, "Failed to get topic identity!")
    val ti = tiOrError.toOption.get
    val config = new Properties()
    config.put(kafka.manager.utils.zero82.LogConfig.RententionMsProp,"1800000")
    val configReadVersion = ti.configReadVersion
    val future = kafkaManager.updateTopicConfig("dev",createTopicNameA,config,configReadVersion)
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    Thread.sleep(2000)

    //check new topic config
    {
      val tiFuture= kafkaManager.getTopicIdentity("dev",createTopicNameA)
      val tiOrError = Await.result(tiFuture, duration)
      assert(tiOrError.isRight, "Failed to get topic identity!")
      val ti = tiOrError.toOption.get
      assert(ti.configReadVersion > configReadVersion)
      assert(ti.config.toMap.apply(kafka.manager.utils.zero82.LogConfig.RententionMsProp) === "1800000")
    }
  }

  test("delete topic") {
    val futureA = kafkaManager.deleteTopic("dev",createTopicNameA)
    val resultA = Await.result(futureA,duration)
    assert(resultA.isRight === true, resultA)
    Thread.sleep(2000)
    val futureA2 = kafkaManager.getTopicList("dev")
    val resultA2 = Await.result(futureA2,duration)
    assert(resultA2.isRight === true, resultA2)
    assert(!resultA2.toOption.get.list.contains(createTopicNameA),"Topic not deleted")

    val futureB = kafkaManager.deleteTopic("dev",createTopicNameB)
    val resultB = Await.result(futureB,duration)
    assert(resultB.isRight === true, resultB)
    Thread.sleep(2000)
    val futureB2 = kafkaManager.getTopicList("dev")
    val resultB2 = Await.result(futureB2,duration)
    assert(resultB2.isRight === true, resultB2)
    assert(!resultB2.toOption.get.list.contains(createTopicNameB),"Topic not deleted")
  }

  test("fail to delete non-existent topic") {
    val future = kafkaManager.deleteTopic("dev","delete_me")
    val result = Await.result(future,duration)
    assert(result.isLeft === true)
  }

  test("update cluster zkhost") {
    val future = kafkaManager.updateCluster("dev","2.4.1",testServer.getConnectString, jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxSsl = false, jmxPass = None, tuning = Option(defaultTuning), securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val result = Await.result(future,duration)
    assert(result.isRight === true)

    Thread.sleep(2000)
    val future2 = kafkaManager.getClusterList
    val result2 = Await.result(future2,duration)
    assert(result2.isRight === true)
    assert((result2.toOption.get.pending.nonEmpty === true) ||
           (result2.toOption.get.active.find(c => c.name == "dev").get.curatorConfig.zkConnect === testServer.getConnectString))
    Thread.sleep(2000)
  }

  test("disable cluster") {
    val future = kafkaManager.disableCluster("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)

    Thread.sleep(2000)
    val future2 = kafkaManager.getClusterList
    val result2 = Await.result(future2,duration)
    assert(result2.isRight === true)
    assert((result2.toOption.get.pending.nonEmpty === true) ||
           (result2.toOption.get.active.find(c => c.name == "dev").get.enabled === false))
    Thread.sleep(2000)
  }

  test("enable cluster") {
    val future = kafkaManager.enableCluster("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    Thread.sleep(2000)
  }

  test("update cluster version") {
    val future = kafkaManager.updateCluster("dev","0.8.1.1",testServer.getConnectString, jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(defaultTuning), securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    Thread.sleep(2000)

    val future2 = kafkaManager.getClusterList
    val result2 = Await.result(future2,duration)
    assert(result2.isRight === true)
    assert((result2.toOption.get.pending.nonEmpty === true) ||
           (result2.toOption.get.active.find(c => c.name == "dev").get.version === Kafka_0_8_1_1))
    Thread.sleep(2000)
  }

  test("delete topic not supported prior to 2.0.0") {
    val future = kafkaManager.deleteTopic("dev",createTopicNameA)
    val result = Await.result(future,duration)
    assert(result.isLeft === true, result)
    assert(result.swap.toOption.get.msg.contains("not supported"))
    Thread.sleep(2000)
  }

  test("update cluster logkafka enabled and activeOffsetCache enabled") {
    val future = kafkaManager.updateCluster("dev","2.4.1",testServer.getConnectString, jmxEnabled = false, pollConsumers = true, filterConsumers = true, logkafkaEnabled = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(defaultTuning), securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    
    Thread.sleep(2000)

    val future2 = kafkaManager.getClusterList
    val result2 = Await.result(future2,duration)
    assert(result2.isRight === true)
    assert((result2.toOption.get.active.find(c => c.name == "dev").get.logkafkaEnabled === true) &&
      (result2.toOption.get.active.find(c => c.name == "dev").get.activeOffsetCacheEnabled === true))
    Thread.sleep(2000)
  }

  test("update cluster security protocol and sasl mechanism") {
    val future = kafkaManager.updateCluster("dev","1.1.0",testServer.getConnectString, jmxEnabled = false, pollConsumers = true, filterConsumers = true, logkafkaEnabled = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(defaultTuning), securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val result = Await.result(future,duration)
    assert(result.isRight === true)

    Thread.sleep(2000)

    val future2 = kafkaManager.getClusterList
    val result2 = Await.result(future2,duration)
    assert(result2.isRight === true)
    assert((result2.toOption.get.active.find(c => c.name == "dev").get.securityProtocol === SASL_PLAINTEXT) &&
      (result2.toOption.get.active.find(c => c.name == "dev").get.saslMechanism === Option(SASL_MECHANISM_PLAIN)))
    Thread.sleep(2000)

    val future3 = kafkaManager.updateCluster("dev","1.1.0",testServer.getConnectString, jmxEnabled = false, pollConsumers = true, filterConsumers = true, logkafkaEnabled = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(defaultTuning), securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val result3 = Await.result(future3,duration)
    assert(result3.isRight === true)

    Thread.sleep(2000)

    val future4 = kafkaManager.getClusterList
    val result4 = Await.result(future4,duration)
    assert(result4.isRight === true)
    assert((result4.toOption.get.active.find(c => c.name == "dev").get.securityProtocol === PLAINTEXT) &&
      (result4.toOption.get.active.find(c => c.name == "dev").get.saslMechanism === None))
    Thread.sleep(2000)
  }

  /*
  test("get consumer list active mode") {
    val future = kafkaManager.getConsumerListExtended("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true, s"Failed : ${result}")
    assert(result.toOption.get.clusterContext.config.activeOffsetCacheEnabled === false, s"Failed : ${result}")
    assert(result.toOption.get.list.head._1 === hlConsumer.get.groupId, s"Failed : ${result}")
  }

  test("get consumer identity active mode") {
    val future = kafkaManager.getConsumerIdentity("dev", hlConsumer.get.groupId)
    val result = Await.result(future,duration)
    assert(result.isRight === true, s"Failed : ${result}")
    assert(result.toOption.get.clusterContext.config.activeOffsetCacheEnabled === false, s"Failed : ${result}")
    assert(result.toOption.get.topicMap.head._1 === seededTopic, s"Failed : ${result}")
  }*/

  test("create logkafka") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp,createLogkafkaTopic)
    val future = kafkaManager.createLogkafka("dev",createLogkafkaLogkafkaId,createLogkafkaLogPath,config)
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    Thread.sleep(2000)
  }

  test("get logkafka identity") {
    val future = kafkaManager.getLogkafkaLogkafkaIdList("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    assert(result.toOption.get.list.nonEmpty === true)
    result.toOption.get.list.foreach { logkafka_id =>
      val future2 = kafkaManager.getLogkafkaIdentity("dev",logkafka_id)
      val result2 = Await.result(future2, duration)
      assert(result2.isRight === true)
    }
  }

  test("update logkafka config") {
    val liFuture= kafkaManager.getLogkafkaIdentity("dev",createLogkafkaLogkafkaId)
    val liOrError = Await.result(liFuture, duration)
    assert(liOrError.isRight, "Failed to get logkafka identity!")
    val li = liOrError.toOption.get
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp,createLogkafkaTopic)
    config.put(kafka.manager.utils.logkafka82.LogConfig.PartitionProp,"1")
    val future = kafkaManager.updateLogkafkaConfig("dev",createLogkafkaLogkafkaId,createLogkafkaLogPath,config)
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    
    Thread.sleep(1000)

    //check new logkafka config
    {
      val liFuture= kafkaManager.getLogkafkaIdentity("dev",createLogkafkaLogkafkaId)
      val liOrError = Await.result(liFuture, duration)
      assert(liOrError.isRight, "Failed to get logkafka identity!")
      val li = liOrError.toOption.get
      assert(li.identityMap.get(createLogkafkaLogPath).get._1.get.apply(kafka.manager.utils.logkafka82.LogConfig.PartitionProp) === "1")
    }
  }

  test("delete logkafka") {
    val future = kafkaManager.deleteLogkafka("dev",createLogkafkaLogkafkaId,createLogkafkaLogPath)
    val result = Await.result(future,duration)
    assert(result.isRight === true, result)
    Thread.sleep(2000)
    val liFuture= kafkaManager.getLogkafkaIdentity("dev",createLogkafkaLogkafkaId)
    val liOrError = Await.result(liFuture, duration)
    assert(liOrError.isRight, "Failed to get logkafka identity!")
    val li = liOrError.toOption.get
    assert(li.identityMap.get(createLogkafkaLogPath) === None)
    Thread.sleep(2000)
  }

  test("delete cluster") {
    //first have to disable in order to delete
    {
      val future = kafkaManager.disableCluster("dev")
      val result = Await.result(future, duration)
      assert(result.isRight === true)
      Thread.sleep(2000)
    }

    val future = kafkaManager.deleteCluster("dev")
    val result = Await.result(future,duration)
    assert(result.isRight === true)
    Thread.sleep(2000)
    val future2 = kafkaManager.getClusterList
    val result2 = Await.result(future2,duration)
    assert(result2.isRight === true)
    assert(result2.toOption.get.pending.isEmpty === true)
    assert(result2.toOption.get.active.isEmpty === true)
  }
}
