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
      ClusterConfig("qa!","0.8.1.1","localhost")
    }
  }

  test("invalid kafka version") {
    intercept[IllegalArgumentException] {
      ClusterConfig("qa","0.8.1","localhost:2181")
    }
  }

  test("case insensitive name") {
    assert(ClusterConfig("QA","0.8.1.1","localhost:2181").name === "qa")
  }

  test("case insensitive zk hosts") {
    assert(ClusterConfig("QA","0.8.1.1","LOCALHOST:2181").curatorConfig.zkConnect === "localhost:2181")
  }

  test("serialize and deserialize") {
    val cc = ClusterConfig("qa","0.8.2.0","localhost:2181")
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("deserialize without version") {
    val cc = ClusterConfig("qa","0.8.2.0","localhost:2181")
    val serialize: String = ClusterConfig.serialize(cc)
    val noverison = serialize.replace(""","kafkaVersion":"0.8.2.0"""","")
    assert(!noverison.contains("kafkaVersion"))
    val deserialize = ClusterConfig.deserialize(noverison)
    assert(deserialize.isSuccess === true)
    assert(cc.copy(version = Kafka_0_8_1_1) == deserialize.get)
  }

  test("deserialize from 0.8.2-beta as 0.8.2.0") {
    val cc = ClusterConfig("qa","0.8.2-beta","localhost:2181")
    val serialize: String = ClusterConfig.serialize(cc)
    val noverison = serialize.replace(""","kafkaVersion":"0.8.2.0"""",""","kafkaVersion":"0.8.2-beta"""")
    val deserialize = ClusterConfig.deserialize(noverison)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }
}
