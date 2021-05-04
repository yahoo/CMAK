/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager.model

import kafka.manager.model.ActorModel.BrokerIdentity
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BrokerIdentityTest extends AnyFunSuite with Matchers {
  test("successfully parse json with endpoints with sasl") {
    val jsonString = """{"jmx_port":9999,"timestamp":"1461773047828","endpoints":["SASL_PLAINTEXT://host.com:9092"],"host":null,"version":2,"port":-1}"""

    val biVal = BrokerIdentity.from(1, jsonString)
    assert(biVal.isSuccess)
    val bi = biVal.toOption.get
    assert(bi.host === "host.com")
    assert(bi.endpoints.contains(SASL_PLAINTEXT))
    assert(bi.endpoints(SASL_PLAINTEXT) === 9092)
    assert(bi.endpointsString === "SASL_PLAINTEXT:9092")
    assert(bi.secure === true)
  }

  test("successfully parse json with endpoints with plaintext") {
    val jsonString = """{"jmx_port":-1,"timestamp":"1462400864268","endpoints":["PLAINTEXT://host.com:9092"],"host":"host.com","version":2,"port":9092}"""

    val biVal = BrokerIdentity.from(1, jsonString)
    assert(biVal.isSuccess)
    val bi = biVal.toOption.get
    assert(bi.host === "host.com")
    assert(bi.endpoints.contains(PLAINTEXT))
    assert(bi.endpoints(PLAINTEXT) === 9092)
    assert(bi.endpointsString === "PLAINTEXT:9092")
    assert(bi.secure === false)

  }

  test("successfully parse json with unparseable endpoints") {
    val jsonString = """{"endpoints":["INSIDE://inside:9092","OUTSIDE://outside.com:9094"],"rack":"us-west-2a","jmx_port":9999,"host":"inside","timestamp":"1501702861441","port":9092,"version":4}"""
    val biVal = BrokerIdentity.from(1, jsonString)
    assert(biVal.isSuccess)
    val bi = biVal.toOption.get
    assert(bi.host === "inside")
    assert(bi.endpoints.contains(PLAINTEXT))
    assert(bi.endpoints(PLAINTEXT) === 9092)
    assert(bi.endpointsString === "PLAINTEXT:9092")
    assert(bi.secure === false)
  }

  test("successfully parse json with endpoints with plaintext and sasl") {
    val jsonString = """{"jmx_port":-1,"timestamp":"1462400864268","endpoints":["PLAINTEXT://host.com:9092", "SASL_PLAINTEXT://host.com:9093"],"host":"host.com","version":2,"port":9092}"""

    val biVal = BrokerIdentity.from(1, jsonString)
    assert(biVal.isSuccess)
    val bi = biVal.toOption.get
    assert(bi.host === "host.com")
    assert(bi.endpoints.contains(PLAINTEXT))
    assert(bi.endpoints.contains(SASL_PLAINTEXT))
    assert(bi.endpoints(PLAINTEXT) === 9092)
    assert(bi.endpoints(SASL_PLAINTEXT) === 9093)
    assert(bi.endpointsString === "SASL_PLAINTEXT:9093,PLAINTEXT:9092")
    assert(bi.secure === true)

  }

  test("successfully parse json without endpoints") {
    val jsonString = """{"jmx_port":-1,"timestamp":"1462400864268","host":"host.com","version":2,"port":9092}"""

    val biVal = BrokerIdentity.from(1, jsonString)
    assert(biVal.isSuccess)
    val bi = biVal.toOption.get
    assert(bi.host === "host.com")
    assert(bi.endpoints.contains(PLAINTEXT))
    assert(bi.endpoints(PLAINTEXT) === 9092)
    assert(bi.endpointsString === "PLAINTEXT:9092")
    assert(bi.secure === false)

  }

  test("successfully parse json with listener names") {
    val jsonString = """{"listener_security_protocol_map":{"PLAINTEXT":"PLAINTEXT","PUBLIC":"SASL_PLAINTEXT"},"endpoints":["PLAINTEXT://host.com:9092","PUBLIC://publichost.com:19092"],"host":"host.com","port":9092,"jmx_port":-1,"version":4}"""

    val biVal = BrokerIdentity.from(1, jsonString)
    assert(biVal.isSuccess)
    val bi = biVal.toOption.get
    assert(bi.host === "host.com")
    assert(bi.endpoints.size === 2)
    assert(bi.endpoints.contains(PLAINTEXT))
    assert(bi.endpoints(PLAINTEXT) === 9092)
    assert(bi.endpoints.contains(SASL_PLAINTEXT))
    assert(bi.endpoints(SASL_PLAINTEXT) === 19092)
    assert(bi.endpointsString === "SASL_PLAINTEXT:19092,PLAINTEXT:9092")
    assert(bi.secure === true)
    assert(bi.nonSecure === true)
  }
}
