/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.test

import com.google.common.io.Files
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.test.InstanceSpec

import java.io.File
import java.util.Properties
import scala.util.Try

/**
 * @author hiral
 */
class KafkaTestBroker(zookeeper: CuratorFramework, zookeeperConnectionString: String) {
  val AdminPath = "/admin"
  val BrokersPath = "/brokers"
  val ClusterPath = "/cluster"
  val ConfigPath = "/config"
  val ControllerPath = "/controller"
  val ControllerEpochPath = "/controller_epoch"
  val IsrChangeNotificationPath = "/isr_change_notification"
  val LogDirEventNotificationPath = "/log_dir_event_notification"
  val KafkaAclPath = "/kafka-acl"
  val KafkaAclChangesPath = "/kafka-acl-changes"

  val ConsumersPath = "/consumers"
  val ClusterIdPath = s"$ClusterPath/id"
  val BrokerIdsPath = s"$BrokersPath/ids"
  val BrokerTopicsPath = s"$BrokersPath/topics"
  val ReassignPartitionsPath = s"$AdminPath/reassign_partitions"
  val DeleteTopicsPath = s"$AdminPath/delete_topics"
  val PreferredReplicaLeaderElectionPath = s"$AdminPath/preferred_replica_election"
  val BrokerSequenceIdPath = s"$BrokersPath/seqid"
  val ConfigChangesPath = s"$ConfigPath/changes"
  val ConfigUsersPath = s"$ConfigPath/users"
  val ConfigBrokersPath = s"$ConfigPath/brokers"
  val ProducerIdBlockPath = "/latest_producer_id_block"

  private[this] val port: Int = InstanceSpec.getRandomPort
  private[this] val config: KafkaConfig = buildKafkaConfig(zookeeperConnectionString)
  private[this] val kafkaServer: KafkaServer = new KafkaServer(config)
  kafkaServer.startup()

  //wait until broker shows up in zookeeper
  var count = 0
  while(count < 10 && zookeeper.checkExists().forPath(BrokerIdsPath + "/0") == null) {
    count += 1
    println("Waiting for broker ...")
    println(Option(zookeeper.getData.forPath(BrokerIdsPath + "/0")).map(kafka.manager.asString))
    Thread.sleep(1000)
  }

  private def buildKafkaConfig(zookeeperConnectionString: String): KafkaConfig = {
    val p: Properties = new Properties
    p.setProperty("zookeeper.connect", zookeeperConnectionString)
    p.setProperty("broker.id", "0")
    p.setProperty("port", "" + port)
    p.setProperty("log.dirs", getLogDir)
    p.setProperty("log.retention.hours", "1")
    p.setProperty("offsets.topic.replication.factor", "1")
    p.setProperty("delete.topic.enable", "true")
    new KafkaConfig(p)
  }

  private def getLogDir: String = {
    val logDir: File = Files.createTempDir
    logDir.deleteOnExit()
    logDir.getAbsolutePath
  }

  def getBrokerConnectionString: String = s"localhost:$port"

  def getPort: Int = port

  def shutdown() {
    Try(kafkaServer.shutdown())
  }
}
