/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager.utils

import kafka.manager.model.{ClusterTuning, Kafka_0_8_1_1, ClusterConfig}
import org.scalatest.{Matchers, FunSuite}

/**
 * @author hiral
 */
class TestClusterConfig extends FunSuite with Matchers {

  test("invalid name") {
    intercept[IllegalArgumentException] {
      ClusterConfig("qa!","0.8.1.1","localhost",jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None)
    }
  }

  test("invalid kafka version") {
    intercept[IllegalArgumentException] {
      ClusterConfig("qa","0.8.1","localhost:2181",jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None)
    }
  }

  test("serialize and deserialize 0.8.1.1") {
    val cc = ClusterConfig("qa","0.8.2.0","localhost:2181", jmxEnabled = true, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 0.8.2.0 +jmx credentials") {
    val cc = ClusterConfig("qa","0.8.2.0","localhost:2181", jmxEnabled = true, jmxUser = Some("mario"), jmxPass = Some("rossi"), jmxSsl = false, pollConsumers = true, filterConsumers = true, tuning = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 0.8.2.0") {
    val cc = ClusterConfig("qa","0.8.2.0","localhost:2181", jmxEnabled = true, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 0.8.2.1") {
    val cc = ClusterConfig("qa","0.8.2.1","localhost:2181", jmxEnabled = true, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 0.8.2.2") {
    val cc = ClusterConfig("qa","0.8.2.2","localhost:2181", jmxEnabled = true, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("deserialize without version and jmxEnabled") {
    val cc = ClusterConfig("qa","0.8.2.0","localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val noverison = serialize.replace(""","kafkaVersion":"0.8.2.0"""","").replace(""","jmxEnabled":false""","").replace(""","jmxSsl":false""","")
    assert(!noverison.contains("kafkaVersion"))
    assert(!noverison.contains("jmxEnabled"))
    assert(!noverison.contains("jmxSsl"))
    val deserialize = ClusterConfig.deserialize(noverison)
    assert(deserialize.isSuccess === true)
    assert(cc.copy(version = Kafka_0_8_1_1) == deserialize.get)
  }

  test("deserialize from 0.8.2-beta as 0.8.2.0") {
    val cc = ClusterConfig("qa","0.8.2-beta","localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val noverison = serialize.replace(""","kafkaVersion":"0.8.2.0"""",""","kafkaVersion":"0.8.2-beta"""")
    val deserialize = ClusterConfig.deserialize(noverison)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("deserialize from 0.9.0.1") {
    val cc = ClusterConfig("qa","0.9.0.1","localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false,
      tuning = Option(ClusterTuning(Option(1),Option(2),Option(3), Option(4), Option(5), Option(6), Option(7), Option(8), Option(9), Option(10), Option(11), Option(12), Option(13), Option(14), Option(15)))
    )
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }
}
