/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager.utils

import kafka.manager.model.{ClusterConfig, ClusterTuning, Kafka_0_8_1_1}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * @author hiral
 */
class TestClusterConfig extends AnyFunSuite with Matchers {

  test("invalid name") {
    intercept[IllegalArgumentException] {
      ClusterConfig("qa!","0.8.1.1","localhost",jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    }
  }

  test("invalid kafka version") {
    intercept[IllegalArgumentException] {
      ClusterConfig("qa","0.8.1","localhost:2181",jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    }
  }

  test("serialize and deserialize 0.8.1.1") {
    val cc = ClusterConfig("qa","0.8.2.0","localhost:2181", jmxEnabled = true, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 0.8.2.0 +jmx credentials") {
    val cc = ClusterConfig("qa","0.8.2.0","localhost:2181", jmxEnabled = true, jmxUser = Some("mario"), jmxPass = Some("rossi"), jmxSsl = false, pollConsumers = true, filterConsumers = true, tuning = None, securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 0.8.2.0") {
    val cc = ClusterConfig("qa","0.8.2.0","localhost:2181", jmxEnabled = true, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 0.8.2.1") {
    val cc = ClusterConfig("qa","0.8.2.1","localhost:2181", jmxEnabled = true, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 0.8.2.2") {
    val cc = ClusterConfig("qa","0.8.2.2","localhost:2181", jmxEnabled = true, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("deserialize without version, jmxEnabled, and security protocol") {
    val cc = ClusterConfig("qa","0.8.2.0","localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
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
    val cc = ClusterConfig("qa","0.8.2-beta","localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val noverison = serialize.replace(""","kafkaVersion":"0.8.2.0"""",""","kafkaVersion":"0.8.2-beta"""")
    val deserialize = ClusterConfig.deserialize(noverison)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("deserialize from 0.9.0.1") {
    val cc = ClusterConfig("qa","0.9.0.1","localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false,
      tuning = Option(ClusterTuning(
        Option(1)
        ,Option(2)
        ,Option(3)
        , Option(4)
        , Option(5)
        , Option(6)
        , Option(7)
        , Option(8)
        , Option(9)
        , Option(10)
        , Option(11)
        , Option(12)
        , Option(13)
        , Option(14)
        , Option(15)
        , Option(16)
        , Option(17)
        , Option(18)
      ))
      , securityProtocol = "PLAINTEXT"
      , saslMechanism = None
      , jaasConfig = None
    )
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 0.10.0.0") {
    val cc = ClusterConfig("qa", "0.10.0.0", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 0.10.1.0") {
    val cc = ClusterConfig("qa", "0.10.1.0", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 0.10.1.1") {
    val cc = ClusterConfig("qa", "0.10.1.1", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 0.10.2.0") {
    val cc = ClusterConfig("qa", "0.10.2.0", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 0.10.2.1") {
    val cc = ClusterConfig("qa", "0.10.2.1", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 0.11.0.0") {
    val cc = ClusterConfig("qa", "0.11.0.0", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 0.11.0.2") {
    val cc = ClusterConfig("qa", "0.11.0.2", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 1.0.0") {
    val cc = ClusterConfig("qa", "1.0.0", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 1.1.0") {
    val cc = ClusterConfig("qa", "1.1.0", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 1.1.1") {
    val cc = ClusterConfig("qa", "1.1.1", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 2.0.0") {
    val cc = ClusterConfig("qa", "2.0.0", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 2.1.0") {
    val cc = ClusterConfig("qa", "2.1.0", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 2.1.1") {
    val cc = ClusterConfig("qa", "2.1.1", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 2.2.0") {
    val cc = ClusterConfig("qa", "2.2.0", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }
  
  test("serialize and deserialize 2.2.1") {
    val cc = ClusterConfig("qa", "2.2.1", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }
  
  test("serialize and deserialize 2.2.2") {
    val cc = ClusterConfig("qa", "2.2.2", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }
  
  test("serialize and deserialize 2.3.0") {
    val cc = ClusterConfig("qa", "2.3.0", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }
  
  test("serialize and deserialize 2.3.1") {
    val cc = ClusterConfig("qa", "2.3.1", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 2.4.0") {
    val cc = ClusterConfig("qa", "2.4.0", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 2.4.1") {
    val cc = ClusterConfig("qa", "2.4.1", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }
  
  test("serialize and deserialize 2.5.0") {
    val cc = ClusterConfig("qa", "2.5.0", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 2.5.1") {
    val cc = ClusterConfig("qa", "2.5.1", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }
  
  test("serialize and deserialize 2.6.0") {
    val cc = ClusterConfig("qa", "2.6.0", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 2.7.0") {
    val cc = ClusterConfig("qa", "2.7.0", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

  test("serialize and deserialize 2.8.0") {
    val cc = ClusterConfig("qa", "2.8.0", "localhost:2181", jmxEnabled = false, pollConsumers = true, filterConsumers = true, activeOffsetCacheEnabled = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = None, securityProtocol = "SASL_PLAINTEXT", saslMechanism = Option("PLAIN"), jaasConfig = Option("blah"))
    val serialize: String = ClusterConfig.serialize(cc)
    val deserialize = ClusterConfig.deserialize(serialize)
    assert(deserialize.isSuccess === true)
    assert(cc == deserialize.get)
  }

}
