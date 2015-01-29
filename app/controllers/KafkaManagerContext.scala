/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import kafka.manager.KafkaManager

/**
 * @author hiral
 */
object KafkaManagerContext {

  import play.api.Play.current

  private[this] val kafkaManager : KafkaManager = new KafkaManager(play.api.Play.configuration.underlying)
  def getKafkaManger : KafkaManager = kafkaManager
  def shutdown() : Unit = {
    kafkaManager.shutdown()
  }
}
