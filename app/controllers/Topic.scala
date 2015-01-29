/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import models.FollowLink
import models.form.CreateTopic
import models.navigation.Menus
import play.api.data.Form
import play.api.data.Forms._
import play.api.data.validation.{Valid, Invalid, Constraint}
import play.api.data.validation.Constraints._
import play.api.mvc._

import scala.concurrent.Future
import scala.util.{Success, Failure, Try}

/**
 * @author hiral
 */
object Topic extends Controller{
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = KafkaManagerContext.getKafkaManger

  val validateName : Constraint[String] = Constraint("validate name") { name =>
    Try {
      kafka.manager.utils.Topic.validate(name)
    } match {
      case Failure(t) => Invalid(t.getMessage)
      case Success(_) => Valid
    }
  }

  val createTopicForm = Form(
    mapping(
      "topic" -> nonEmptyText.verifying(maxLength(250), validateName),
      "partitions" -> number(min=1, max=10000),
      "replication" -> number(min=1, max=10)
    )(CreateTopic.apply)(CreateTopic.unapply)
  )

  def createTopic(clusterName: String) = Action.async { implicit request =>
    Future.successful(Ok(views.html.topic.createTopic(clusterName, createTopicForm)))
  }

  def handleCreateTopic(clusterName: String) = Action.async { implicit request =>
    createTopicForm.bindFromRequest.fold(
      formWithErrors => Future.successful(BadRequest(views.html.topic.createTopic(clusterName,formWithErrors))),
      ct => {
        kafkaManager.createTopic(clusterName,ct.topic,ct.partitions,ct.replication).map { errorOrSuccess =>
          Ok(views.html.common.resultOfCommand(
            views.html.navigation.clusterMenu(clusterName,"Topic","Create",Menus.clusterMenus(clusterName)),
            models.navigation.BreadCrumbs.withNamedViewAndCluster("Topics",clusterName,"Create Topic"),
            errorOrSuccess,
            "Create Topic",
            FollowLink("Go to topic view.",routes.Application.topic(clusterName, ct.topic).toString()),
            FollowLink("Try again.",routes.Topic.createTopic(clusterName).toString())
          ))
        }
      }
    )
  }
}
