/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import features.ApplicationFeatures
import models.navigation.Menus
import play.api.i18n.{MessagesApi, I18nSupport}
import play.api.mvc._

/**
 * @author hiral
 */
class Application (val messagesApi: MessagesApi, val kafkaManagerContext: KafkaManagerContext)
                  (implicit af: ApplicationFeatures, menus: Menus) extends Controller with I18nSupport {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = kafkaManagerContext.getKafkaManager

  def index = Action.async {
    kafkaManager.getClusterList.map { errorOrClusterList =>
      Ok(views.html.index(errorOrClusterList))
    }
  }
}
