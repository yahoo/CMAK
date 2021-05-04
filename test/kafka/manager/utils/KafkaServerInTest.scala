/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager.utils

import kafka.manager.model.CuratorConfig
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * @author hiral
 */
trait KafkaServerInTest extends AnyFunSuite with BeforeAndAfterAll {
  val kafkaServerZkPath : String

  lazy val sharedCurator: CuratorFramework = {
    val config = CuratorConfig(kafkaServerZkPath)
    val curator: CuratorFramework = CuratorFrameworkFactory.newClient(
      config.zkConnect,
      new BoundedExponentialBackoffRetry(config.baseSleepTimeMs, config.maxSleepTimeMs, config.zkMaxRetry))
    curator
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sharedCurator.start()
  }

  override protected def afterAll(): Unit = {
    sharedCurator.close()
    super.afterAll()
  }
}
