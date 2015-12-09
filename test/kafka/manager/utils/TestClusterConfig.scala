/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager.utils

import kafka.manager.{Kafka_0_8_1_1, ClusterConfig}
import org.scalatest.{Matchers, FunSuite}

/**
 * @author hiral
 */
class TestClusterConfig extends FunSuite with Matchers {

  test("invalid name") {
    intercept[IllegalArgumentException] {
      ClusterConfig("qa!","0.8.1.1","localhost",jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None)
    }
  }

  test("invalid kafka version") {
    intercept[IllegalArgumentException] {
      ClusterConfig("qa","0.8.1","localhost:2181",jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None)
    }
  }

  test("serialize and deserialize") {
    val cc = ClusterConfig("qa","0.8.2.0","localhost:2181", jmxEnabled = true, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize +jmx credentials") {
    val cc = ClusterConfig("qa","0.8.2.0","localhost:2181", jmxEnabled = true, jmxUser = Some("mario"), jmxPass = Some("rossi"), pollConsumers = true, filterConsumers = true)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("deserialize without version and jmxEnabled") {
    val cc = ClusterConfig("qa","0.8.2.0","localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val noverison = serialize.replace(""","kafkaVersion":"0.8.2.0"""","")
    assert(!noverison.contains("kafkaVersion"))
    val deserialize = ClusterConfig.deserialize(noverison)
    assert(deserialize.isSuccess === true)
    assert(cc.copy(version = Kafka_0_8_1_1) == deserialize.get)
  }

  test("deserialize from 0.8.2-beta as 0.8.2.0") {
    val cc = ClusterConfig("qa","0.8.2-beta","localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val noverison = serialize.replace(""","kafkaVersion":"0.8.2.0"""",""","kafkaVersion":"0.8.2-beta"""")
    val deserialize = ClusterConfig.deserialize(noverison)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }
}
