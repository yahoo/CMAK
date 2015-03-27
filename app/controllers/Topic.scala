/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import java.util.Properties

import kafka.manager.utils.TopicConfigs
import kafka.manager.{ApiError, Kafka_0_8_2_0, Kafka_0_8_1_1}
import models.FollowLink
import models.form.{DeleteTopic, TConfig, CreateTopic}
import models.navigation.Menus
import play.api.data.Form
import play.api.data.Forms._
import play.api.data.validation.{Valid, Invalid, Constraint}
import play.api.data.validation.Constraints._
import play.api.mvc._

import scala.concurrent.Future
import scala.util.{Success, Failure, Try}
import scalaz.{\/-, -\/}

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
  
  val kafka_0_8_1_1_Default = CreateTopic("",1,1,TopicConfigs.configNames(Kafka_0_8_1_1).map(n => TConfig(n,None)).toList)
  val kafka_0_8_2_0_Default = CreateTopic("",1,1,TopicConfigs.configNames(Kafka_0_8_2_0).map(n => TConfig(n,None)).toList)
  
  val defaultCreateForm = Form(
    mapping(
      "topic" -> nonEmptyText.verifying(maxLength(250), validateName),
      "partitions" -> number(min = 1, max = 10000),
      "replication" -> number(min = 1, max = 10),
      "configs" -> list(
        mapping(
          "name" -> nonEmptyText,
          "value" -> optional(text)
        )(TConfig.apply)(TConfig.unapply)
      )
    )(CreateTopic.apply)(CreateTopic.unapply)
  )
  
  val defaultDeleteForm = Form(
    mapping(
      "topic" -> nonEmptyText.verifying(maxLength(250), validateName)
    )(DeleteTopic.apply)(DeleteTopic.unapply)
  )

  private def createTopicForm(clusterName: String) = {
    kafkaManager.getClusterConfig(clusterName).map { errorOrConfig =>
      errorOrConfig.map { clusterConfig =>
        clusterConfig.version match {
          case Kafka_0_8_1_1 => defaultCreateForm.fill(kafka_0_8_1_1_Default)
          case Kafka_0_8_2_0 => defaultCreateForm.fill(kafka_0_8_2_0_Default)
        }
      }
    }
  }

  def topics(c: String) = Action.async {
    kafkaManager.getTopicList(c).map { errorOrTopicList =>
      Ok(views.html.topic.topicList(c,errorOrTopicList))
    }
  }

  def topic(c: String, t: String) = Action.async {
    kafkaManager.getTopicIdentity(c,t).map { errorOrTopicIdentity =>
      Ok(views.html.topic.topicView(c,t,errorOrTopicIdentity))
    }
  }

  def createTopic(clusterName: String) = Action.async { implicit request =>
    createTopicForm(clusterName).map { errorOrForm =>
      Ok(views.html.topic.createTopic(clusterName, errorOrForm))
    }
  }

  def handleCreateTopic(clusterName: String) = Action.async { implicit request =>
    defaultCreateForm.bindFromRequest.fold(
      formWithErrors => Future.successful(BadRequest(views.html.topic.createTopic(clusterName,\/-(formWithErrors)))),
      ct => {
        val props = new Properties()
        ct.configs.filter(_.value.isDefined).foreach(c => props.setProperty(c.name,c.value.get))
        kafkaManager.createTopic(clusterName,ct.topic,ct.partitions,ct.replication,props).map { errorOrSuccess =>
          Ok(views.html.common.resultOfCommand(
            views.html.navigation.clusterMenu(clusterName,"Topic","Create",Menus.clusterMenus(clusterName)),
            models.navigation.BreadCrumbs.withNamedViewAndCluster("Topics",clusterName,"Create Topic"),
            errorOrSuccess,
            "Create Topic",
            FollowLink("Go to topic view.",routes.Topic.topic(clusterName, ct.topic).toString()),
            FollowLink("Try again.",routes.Topic.createTopic(clusterName).toString())
          ))
        }
      }
    )
  }

  def handleDeleteTopic(clusterName: String, topic: String) = Action.async { implicit request =>
    defaultDeleteForm.bindFromRequest.fold(
      formWithErrors => Future.successful(
        BadRequest(views.html.topic.topicView(
          clusterName,
          topic,
          -\/(ApiError(formWithErrors.error("topic").map(_.toString).getOrElse("Unknown error deleting topic!")))))),
      deleteTopic => {
        kafkaManager.deleteTopic(clusterName,deleteTopic.topic).map { errorOrSuccess =>
          Ok(views.html.common.resultOfCommand(
            views.html.navigation.clusterMenu(clusterName,"Topic","Topic View",Menus.clusterMenus(clusterName)),
            models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View",clusterName,topic,"Delete Topic"),
            errorOrSuccess,
            "Delete Topic",
            FollowLink("Go to topic list.",routes.Topic.topics(clusterName).toString()),
            FollowLink("Try again.",routes.Topic.topic(clusterName, topic).toString())
          ))
        }
      }
    )
  }

}
