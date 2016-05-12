/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager.model

import kafka.manager.model.ActorModel.BrokerIdentity
import org.scalatest.{Matchers, FunSuite}

class BrokerIdentityTest extends FunSuite with Matchers {
  test("successfully parse json with endpoints with sasl") {
    val jsonString = """{"jmx_port":9999,"timestamp":"1461773047828","endpoints":["SASL_PLAINTEXT://host.com:9092"],"host":null,"version":2,"port":-1}"""

    val biVal = BrokerIdentity.from(1, jsonString)
    assert(biVal.isSuccess)
    val bi = biVal.toOption.get
    assert(bi.host === "host.com")
    assert(bi.port === 9092)
    assert(bi.secure === true)
  }

  test("successfully parse json with endpoints with plaintext") {
    val jsonString = """{"jmx_port":-1,"timestamp":"1462400864268","endpoints":["PLAINTEXT://host.com:9092"],"host":"host.com","version":2,"port":9092}"""

    val biVal = BrokerIdentity.from(1, jsonString)
    assert(biVal.isSuccess)
    val bi = biVal.toOption.get
    assert(bi.host === "host.com")
    assert(bi.port === 9092)
    assert(bi.secure === false)

  }

  test("successfully parse json without endpoints") {
    val jsonString = """{"jmx_port":-1,"timestamp":"1462400864268","host":"host.com","version":2,"port":9092}"""

    val biVal = BrokerIdentity.from(1, jsonString)
    assert(biVal.isSuccess)
    val bi = biVal.toOption.get
    assert(bi.host === "host.com")
    assert(bi.port === 9092)
    assert(bi.secure === false)

  }
}
