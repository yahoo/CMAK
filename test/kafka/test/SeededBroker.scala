/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.test

import grizzled.slf4j.Logging
import kafka.manager.model.Kafka_1_1_0
import kafka.manager.utils.AdminUtils
import kafka.message.{DefaultCompressionCodec, NoCompressionCodec}
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingServer
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{ForeachAction, KStream}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}

import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Properties, UUID}
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
  
  private[this] val adminUtils = new AdminUtils(Kafka_1_1_0)

  //seed with table
  {
    adminUtils.createTopic(zookeeper, Set(0),seedTopic,partitions,1)
    Thread.sleep(5000)
    require(adminUtils.topicExists(zookeeper, seedTopic), "Failed to create seed topic!")
  }

  private[this] val commonConsumerConfig = new Properties()
  commonConsumerConfig.put(BOOTSTRAP_SERVERS_CONFIG, getBrokerConnectionString)
  commonConsumerConfig.put(REQUEST_TIMEOUT_MS_CONFIG, "11000")
  commonConsumerConfig.put(SESSION_TIMEOUT_MS_CONFIG, "10000")
  commonConsumerConfig.put(RECEIVE_BUFFER_CONFIG, s"${64 * 1024}")
  commonConsumerConfig.put(CLIENT_ID_CONFIG, "test-consumer")

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
  
  def getKafkaConsumer = {
    new KafkaConsumer(commonConsumerConfig)
  }
  
  def getHighLevelConsumer : HighLevelConsumer = {
    new HighLevelConsumer(seedTopic, "test-hl-consumer", commonConsumerConfig)
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
 * @param commonConsumerConfig
 * @param readFromStartOfStream
 */
case class HighLevelConsumer(topic: String,
                    groupId: String,
                    commonConsumerConfig: Properties,
                    readFromStartOfStream: Boolean = true) extends Logging {

  commonConsumerConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, groupId)
  commonConsumerConfig.put(StreamsConfig.CLIENT_ID_CONFIG, groupId)
  commonConsumerConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass.getName)
  commonConsumerConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass.getName)
  commonConsumerConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100" )
  commonConsumerConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")

  info("setup:start topic=%s for bk=%s and groupId=%s".format(topic,commonConsumerConfig.getProperty(BOOTSTRAP_SERVERS_CONFIG),groupId))
  val streamsBuilder = new StreamsBuilder
  val kstream : KStream[Array[Byte], Array[Byte]] = streamsBuilder.stream(topic)

  val kafkaStreams = new KafkaStreams(streamsBuilder.build(), commonConsumerConfig)
  kafkaStreams.setUncaughtExceptionHandler(new MaxFailuresUncaughtExceptionHandler(3, 3600000 ))

  kafkaStreams.start()
  info("setup:complete topic=%s for bk=%s and groupId=%s".format(topic,commonConsumerConfig.getProperty(BOOTSTRAP_SERVERS_CONFIG),groupId))


  def read(write: (Array[Byte])=>Unit) = {
    info("reading on stream now")
    kstream.foreach(new ForeachAction[Array[Byte], Array[Byte]] {
      def apply(k:Array[Byte], v:Array[Byte]): Unit = {
        try {
          info("writing from stream")
          write(v)
          info("written to stream")
        } catch {
          case e: Throwable =>
            error("Error processing message, skipping this message: ", e)
        }
      }
    })
  }

  def close() {
    kafkaStreams.close()
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
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

  props.put("batch.num.messages", batchSize.toString)
  props.put("message.send.max.retries", messageSendMaxRetries.toString)
  props.put("request.required.acks",requestRequiredAcks.toString)
  props.put("client.id",clientId.toString)

  val producer = new KafkaProducer[AnyRef, AnyRef](props)

  def send(message: String, partition: Integer): Unit = send(message.getBytes("UTF8"), partition)

  def send(message: Array[Byte], partition: Integer): Unit = {
    try {
      val future = producer.send(new ProducerRecord[AnyRef,AnyRef](topic, partition, null, message))
      if(synchronously) future.get()
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

  info("setup:start topic=%s for broker=%s and groupId=%s".format(topic,brokerConnect,groupId))
  consumer.subscribe(java.util.Arrays.asList(topic))
  info("setup:complete topic=%s for zk=%s and groupId=%s".format(topic,brokerConnect,groupId))

  def read(write: (String)=>Unit) = {
    import collection.JavaConverters._
    while (true) {
      val records : ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(pollMillis))
      for(record <- records.asScala) {
        write(record.value())
      }
    }
  }

  def close() {
    consumer.close()
  }
}