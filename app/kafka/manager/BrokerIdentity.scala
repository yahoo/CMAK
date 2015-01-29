/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import kafka.manager.ActorModel.BrokerInfo

/**
 * @author hiral
 */
case class BrokerIdentity(id: String, host: String, port: Int)

object BrokerIdentity {
  import play.api.libs.json._

  implicit def from(info: BrokerInfo): BrokerIdentity = {
    val config = Json.parse(info.config)
    BrokerIdentity(info.id, (config \ "host").as[String], (config \ "port").as[Int])
  }
}
