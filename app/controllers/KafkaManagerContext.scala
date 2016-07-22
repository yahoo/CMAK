/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import kafka.manager.KafkaManager
import play.api.Configuration
import play.api.inject.ApplicationLifecycle

import scala.concurrent.Future

/**
 * @author hiral
 */
class KafkaManagerContext (lifecycle: ApplicationLifecycle, configuration: Configuration) {

  private[this] val kafkaManager : KafkaManager = new KafkaManager(configuration.underlying)
  
  lifecycle.addStopHook { () =>
    Future.successful(kafkaManager.shutdown())
  }

  def getKafkaManager : KafkaManager = kafkaManager
}
