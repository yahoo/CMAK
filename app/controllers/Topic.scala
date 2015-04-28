/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import java.util.Properties

import kafka.manager.ActorModel.TopicIdentity
import kafka.manager.utils.TopicConfigs
import kafka.manager.{ApiError, Kafka_0_8_2_0, Kafka_0_8_1_1}
import models.FollowLink
import models.form._
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
      "replication" -> number(min = 1, max = 1000),
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

  val defaultAddPartitionsForm = Form(
    mapping(
      "topic" -> nonEmptyText.verifying(maxLength(250), validateName),
      "brokers" -> seq {
        mapping(
          "id" -> number(min = 0),
          "host" -> nonEmptyText,
          "selected" -> boolean
        )(BrokerSelect.apply)(BrokerSelect.unapply)
      },
      "partitions" -> number(min = 1, max = 10000),
      "readVersion" -> number(min = 0)
    )(AddTopicPartitions.apply)(AddTopicPartitions.unapply)
  )

  val defaultUpdateConfigForm = Form(
    mapping(
      "topic" -> nonEmptyText.verifying(maxLength(250), validateName),
      "configs" -> list(
        mapping(
          "name" -> nonEmptyText,
          "value" -> optional(text)
        )(TConfig.apply)(TConfig.unapply)
      ),
      "readVersion" -> number(min = 0)
    )(UpdateTopicConfig.apply)(UpdateTopicConfig.unapply)
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

  def addPartitions(clusterName: String, topic: String) = Action.async { implicit request =>
    val errorOrFormFuture = kafkaManager.getTopicIdentity(clusterName, topic).flatMap { errorOrTopicIdentity =>
      errorOrTopicIdentity.fold( e => Future.successful(-\/(e)),{ topicIdentity =>
        kafkaManager.getBrokerList(clusterName).map { errorOrBrokerList =>
          errorOrBrokerList.map { bl =>
            defaultAddPartitionsForm.fill(AddTopicPartitions(topic,bl.map(bi => BrokerSelect.from(bi)),topicIdentity.partitions,topicIdentity.readVersion))
          }
        }
      })
    }
    errorOrFormFuture.map { errorOrForm =>
      Ok(views.html.topic.addPartitions(clusterName, topic, errorOrForm))
    }
  }

  def handleAddPartitions(clusterName: String, topic: String) = Action.async { implicit request =>
    defaultAddPartitionsForm.bindFromRequest.fold(
      formWithErrors => Future.successful(BadRequest(views.html.topic.addPartitions(clusterName, topic,\/-(formWithErrors)))),
      addTopicPartitions => {
        kafkaManager.addTopicPartitions(clusterName,addTopicPartitions.topic,addTopicPartitions.brokers.filter(_.selected).map(_.id),addTopicPartitions.partitions,addTopicPartitions.readVersion).map { errorOrSuccess =>
          Ok(views.html.common.resultOfCommand(
            views.html.navigation.clusterMenu(clusterName,"Topic","Topic View",Menus.clusterMenus(clusterName)),
            models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View",clusterName, topic,"Add Partitions"),
            errorOrSuccess,
            "Add Partitions",
            FollowLink("Go to topic view.",routes.Topic.topic(clusterName, addTopicPartitions.topic).toString()),
            FollowLink("Try again.",routes.Topic.addPartitions(clusterName, topic).toString())
          ))
        }
      }
    )
  }

  private def updateConfigForm(clusterName: String, ti: TopicIdentity) = {
    kafkaManager.getClusterConfig(clusterName).map { errorOrConfig =>
      errorOrConfig.map { clusterConfig =>
        val defaultConfigMap = clusterConfig.version match {
          case Kafka_0_8_1_1 => TopicConfigs.configNames(Kafka_0_8_1_1).map(n => (n,TConfig(n,None))).toMap
          case Kafka_0_8_2_0 => TopicConfigs.configNames(Kafka_0_8_2_0).map(n => (n,TConfig(n,None))).toMap
        }
        val combinedMap = defaultConfigMap ++ ti.config.toMap.map(tpl => tpl._1 -> TConfig(tpl._1,Option(tpl._2)))
        defaultUpdateConfigForm.fill(UpdateTopicConfig(ti.topic,combinedMap.toList.map(_._2),ti.configReadVersion))
      }
    }
  }

  def updateConfig(clusterName: String, topic: String) = Action.async { implicit request =>
    val errorOrFormFuture = kafkaManager.getTopicIdentity(clusterName, topic).flatMap { errorOrTopicIdentity =>
      errorOrTopicIdentity.fold( e => Future.successful(-\/(e)) ,{ topicIdentity =>
        updateConfigForm(clusterName, topicIdentity)
      })
    }
    errorOrFormFuture.map { errorOrForm =>
      Ok(views.html.topic.updateConfig(clusterName, topic, errorOrForm))
    }
  }

  def handleUpdateConfig(clusterName: String, topic: String) = Action.async { implicit request =>
    defaultUpdateConfigForm.bindFromRequest.fold(
      formWithErrors => Future.successful(BadRequest(views.html.topic.updateConfig(clusterName, topic,\/-(formWithErrors)))),
      updateTopicConfig => {
        val props = new Properties()
        updateTopicConfig.configs.filter(_.value.isDefined).foreach(c => props.setProperty(c.name,c.value.get))
        kafkaManager.updateTopicConfig(clusterName,updateTopicConfig.topic,props,updateTopicConfig.readVersion).map { errorOrSuccess =>
          Ok(views.html.common.resultOfCommand(
            views.html.navigation.clusterMenu(clusterName,"Topic","Topic View",Menus.clusterMenus(clusterName)),
            models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View",clusterName, topic,"Update Config"),
            errorOrSuccess,
            "Update Config",
            FollowLink("Go to topic view.",routes.Topic.topic(clusterName, updateTopicConfig.topic).toString()),
            FollowLink("Try again.",routes.Topic.updateConfig(clusterName, topic).toString())
          ))
        }
      }
    )
  }
}
