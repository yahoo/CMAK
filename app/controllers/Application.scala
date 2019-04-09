/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import features.ApplicationFeatures
import models.navigation.Menus
import play.api.i18n.I18nSupport
import play.api.mvc._

import scala.concurrent.ExecutionContext

/**
 * @author hiral
 */
class Application(val cc: ControllerComponents, kafkaManagerContext: KafkaManagerContext)
                 (implicit af: ApplicationFeatures, menus: Menus, ec:ExecutionContext) extends AbstractController(cc) with I18nSupport {

  private[this] val kafkaManager = kafkaManagerContext.getKafkaManager

  def index = Action.async { implicit request: RequestHeader =>
    kafkaManager.getClusterList.map { errorOrClusterList =>
      Ok(views.html.index(errorOrClusterList)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
    }
  }
}
