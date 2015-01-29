/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.test

import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingServer

import scala.util.Try

/**
 * @author hiral
 */
class SeededBroker(seedTopic: String, partitions: Int) {
  private[this] val maxRetry = 100
  private[this] val testingServer = getTestingServer(10000)
  private[this] val zookeeperConnectionString: String = testingServer.getConnectString
  private[this] val retryPolicy: ExponentialBackoffRetry = new ExponentialBackoffRetry(1000, 3)
  private[this] final val zookeeper: CuratorFramework =
    CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)
  zookeeper.start()
  private[this] val broker = new KafkaTestBroker(zookeeper,zookeeperConnectionString)

  //seed with table
  {
    kafka.manager.utils.AdminUtils.createTopic(zookeeper,IndexedSeq(0),seedTopic,partitions,1)
  }

  private def getTestingServer(startPort: Int) : TestingServer = {
    var count = 0;
    while(count < maxRetry) {
      val port = startPort + count
      val result = initTestingServer(port)
      if(result.isSuccess)
        return result.get
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
}
