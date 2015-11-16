/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers.api

import controllers.KafkaManagerContext
import controllers.Topic._
import play.api.libs.json._
import play.api.mvc._

object Topic extends Controller {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = KafkaManagerContext.getKafkaManager

  def index(c: String) = Action.async { implicit request =>
    kafkaManager.getTopicListExtended(c).map { errorOrTopicList =>
      errorOrTopicList.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        topicList => Ok(Json.obj("topicList" -> topicList.list.map(bi => bi._1)))
      )
    }
  }
}

