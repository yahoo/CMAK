/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package models.form

/**
 * @author liu qiang
 */

sealed trait BrokerOperation

case class BConfig(name: String, value: Option[String], help: Option[String])

case class UpdateBrokerConfig(broker: Int, configs: List[BConfig], readVersion: Int) extends BrokerOperation

