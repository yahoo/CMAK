/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.test

import java.util.{UUID, Properties}
import java.util.concurrent.atomic.AtomicInteger

import grizzled.slf4j.Logging
import kafka.consumer._
import kafka.manager.model.Kafka_0_8_2_0
import kafka.manager.utils.AdminUtils
import kafka.message.{NoCompressionCodec, DefaultCompressionCodec}
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import kafka.serializer.DefaultDecoder
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingServer
import org.apache.kafka.clients.consumer.{ConsumerRecords, ConsumerRecord, KafkaConsumer}

import scala.util.Try

/**
 * @author hiral
 */
class SeededBroker(seedTopic: String, partitions: Int) {
  private[this] val maxRetry = 100
  private[this] val testingServer = getTestingServer
  private[this] val zookeeperConnectionString: String = testingServer.getConnectString
  private[this] val retryPolicy: ExponentialBackoffRetry = new ExponentialBackoffRetry(1000, 3)
  private[this] final val zookeeper: CuratorFramework =
    CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)
  zookeeper.start()
  private[this] val broker = new KafkaTestBroker(zookeeper,zookeeperConnectionString)
  
  private[this] val adminUtils = new AdminUtils(Kafka_0_8_2_0)

  //seed with table
  {
    adminUtils.createTopic(zookeeper, Set(0),seedTopic,partitions,1)
  }

  private def getTestingServer : TestingServer = {
    var count = 0
    while(count < maxRetry) {
      val port = SeededBroker.nextPortNum()
      val result = initTestingServer(port)
      if(result.isSuccess)
        return result.get
      count += 1
    }
    throw new RuntimeException("Failed to create testing server using curator!")
  }
  
  private def initTestingServer(port: Int) : Try[TestingServer] = {
    Try(new TestingServer(port,true))
  }

  def getBrokerConnectionString = broker.getBrokerConnectionString
  def getZookeeperConnectionString = testingServer.getConnectString

  def shutdown(): Unit = {
    Try(broker.shutdown())
    Try {
      if (zookeeper.getState == CuratorFrameworkState.STARTED) {
        zookeeper.close()
      }
    }
    Try(testingServer.close())
  }
  
  def getSimpleConsumer : SimpleConsumer = {
    new SimpleConsumer("localhost", broker.getPort, 10000, 64 * 1024, "test-consumer")
  }
  
  def getHighLevelConsumer : HighLevelConsumer = {
    new HighLevelConsumer(seedTopic, "test-hl-consumer", getZookeeperConnectionString)
  }

  def getNewConsumer : NewKafkaManagedConsumer = {
    new NewKafkaManagedConsumer(seedTopic, "test-new-consumer", getBrokerConnectionString)
  }

  def getSimpleProducer : SimpleProducer = {
    new SimpleProducer(seedTopic, getBrokerConnectionString, "test-producer")
    
  }
}

object SeededBroker {
  val portNum = new AtomicInteger(10000)
  
  def nextPortNum(): Int = portNum.incrementAndGet()
}

/**
 * Borrowed from https://github.com/stealthly/scala-kafka/blob/master/src/main/scala/KafkaConsumer.scala
  *
  * @param topic
 * @param groupId
 * @param zookeeperConnect
 * @param readFromStartOfStream
 */
case class HighLevelConsumer(topic: String,
                    groupId: String,
                    zookeeperConnect: String,
                    readFromStartOfStream: Boolean = true) extends Logging {

  val props = new Properties()
  props.put("group.id", groupId)
  props.put("zookeeper.connect", zookeeperConnect)
  props.put("zookeeper.sync.time.ms", "200")
  props.put("auto.commit.interval.ms", "1000")
  props.put("auto.offset.reset", if(readFromStartOfStream) "smallest" else "largest")

  val config = new ConsumerConfig(props)
  val connector = Consumer.create(config)

  val filterSpec = new Whitelist(topic)

  info("setup:start topic=%s for zk=%s and groupId=%s".format(topic,zookeeperConnect,groupId))
  val stream : KafkaStream[Array[Byte], Array[Byte]] = connector.createMessageStreamsByFilter[Array[Byte], Array[Byte]](filterSpec, 1, new DefaultDecoder(), new DefaultDecoder()).head
  info("setup:complete topic=%s for zk=%s and groupId=%s".format(topic,zookeeperConnect,groupId))

  def read(write: (Array[Byte])=>Unit) = {
    info("reading on stream now")
    for(messageAndTopic <- stream) {
      try {
        info("writing from stream")
        write(messageAndTopic.message)
        info("written to stream")
      } catch {
        case e: Throwable =>
            error("Error processing message, skipping this message: ", e)
      }
    }
  }

  def close() {
    connector.shutdown()
  }
}

/**
 * Borrowed from https://github.com/stealthly/scala-kafka/blob/master/src/main/scala/KafkaProducer.scala
  *
  * @param topic
 * @param brokerList
 * @param clientId
 * @param synchronously
 * @param compress
 * @param batchSize
 * @param messageSendMaxRetries
 * @param requestRequiredAcks
 */
case class SimpleProducer(topic: String,
                         brokerList: String,
                         clientId: String = UUID.randomUUID().toString,
                         synchronously: Boolean = true,
                         compress: Boolean = true,
                         batchSize: Integer = 200,
                         messageSendMaxRetries: Integer = 3,
                         requestRequiredAcks: Integer = -1
                          ) {

  val props = new Properties()

  val codec = if(compress) DefaultCompressionCodec.codec else NoCompressionCodec.codec

  props.put("compression.codec", codec.toString)
  props.put("producer.type", if(synchronously) "sync" else "async")
  props.put("metadata.broker.list", brokerList)
  props.put("batch.num.messages", batchSize.toString)
  props.put("message.send.max.retries", messageSendMaxRetries.toString)
  props.put("request.required.acks",requestRequiredAcks.toString)
  props.put("client.id",clientId.toString)

  val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(props))

  def kafkaMesssage(message: Array[Byte], partition: Array[Byte]): KeyedMessage[AnyRef, AnyRef] = {
    if (partition == null) {
      new KeyedMessage(topic,message)
    } else {
      new KeyedMessage(topic,partition,message)
    }
  }

  def send(message: String, partition: String = null): Unit = send(message.getBytes("UTF8"), if (partition == null) null else partition.getBytes("UTF8"))

  def send(message: Array[Byte], partition: Array[Byte]): Unit = {
    try {
      producer.send(kafkaMesssage(message, partition))
    } catch {
      case e: Exception =>
        e.printStackTrace
        System.exit(1)
    }
  }
}

case class NewKafkaManagedConsumer(topic: String,
                                   groupId: String,
                                   brokerConnect: String,
                                   pollMillis: Int = 100,
                                   readFromStartOfStream: Boolean = true) extends Logging {

  val props = new Properties()
  props.put("bootstrap.servers", brokerConnect)
  props.put("group.id", groupId)
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("session.timeout.ms", "30000")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("auto.offset.reset", if(readFromStartOfStream) "earliest" else "latest")

  val consumer = new KafkaConsumer[String, String](props)

  val filterSpec = new Whitelist(topic)

  info("setup:start topic=%s for broker=%s and groupId=%s".format(topic,brokerConnect,groupId))
  consumer.subscribe(java.util.Arrays.asList(topic))
  info("setup:complete topic=%s for zk=%s and groupId=%s".format(topic,brokerConnect,groupId))

  def read(write: (String)=>Unit) = {
    import collection.JavaConverters._
    while (true) {
      val records : ConsumerRecords[String, String] = consumer.poll(pollMillis)
      for(record <- records.asScala) {
        write(record.value())
      }
    }
  }

  def close() {
    consumer.close()
  }
}