/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

import controllers.KafkaManagerContext
import kafka.manager.KafkaManager
import play.api._

/**
 * @author hiral
 */
object GlobalKafkaManager extends GlobalSettings {

  private[this] var kafkaManager: KafkaManager = null

  override def beforeStart(app: Application): Unit = {
    Logger.info("Init kafka manager...")
    KafkaManagerContext.getKafkaManger
    Thread.sleep(5000)
  }

  override def onStop(app: Application) {
    KafkaManagerContext.shutdown()
    Logger.info("Application shutdown...")
  }
}


