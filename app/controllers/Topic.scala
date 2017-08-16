/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import java.util.Properties

import features.{ApplicationFeatures, KMTopicManagerFeature}
import kafka.manager.ApiError
import kafka.manager.features.ClusterFeatures
import kafka.manager.model.ActorModel.TopicIdentity
import kafka.manager.model._
import kafka.manager.utils.TopicConfigs
import models.FollowLink
import models.form.ReassignPartitionOperation.{ForceRunAssignment, RunAssignment}
import models.form._
import models.navigation.Menus
import play.api.data.Form
import play.api.data.Forms._
import play.api.data.validation.Constraints._
import play.api.data.validation.{Constraint, Invalid, Valid}
import play.api.i18n.{I18nSupport, MessagesApi}
import play.api.mvc._

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scalaz.-\/

/**
 * @author hiral
 */
class Topic (val messagesApi: MessagesApi, val kafkaManagerContext: KafkaManagerContext)
            (implicit af: ApplicationFeatures, menus: Menus) extends Controller with I18nSupport {
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = kafkaManagerContext.getKafkaManager

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
  val kafka_0_8_2_1_Default = CreateTopic("",1,1,TopicConfigs.configNames(Kafka_0_8_2_1).map(n => TConfig(n,None)).toList)
  val kafka_0_8_2_2_Default = CreateTopic("",1,1,TopicConfigs.configNames(Kafka_0_8_2_2).map(n => TConfig(n,None)).toList)
  val kafka_0_9_0_0_Default = CreateTopic("",1,1,TopicConfigs.configNames(Kafka_0_9_0_0).map(n => TConfig(n,None)).toList)
  val kafka_0_9_0_1_Default = CreateTopic("",1,1,TopicConfigs.configNames(Kafka_0_9_0_1).map(n => TConfig(n,None)).toList)
  val kafka_0_10_0_0_Default = CreateTopic("",1,1,TopicConfigs.configNames(Kafka_0_10_0_0).map(n => TConfig(n,None)).toList)
  val kafka_0_10_0_1_Default = CreateTopic("",1,1,TopicConfigs.configNames(Kafka_0_10_0_1).map(n => TConfig(n,None)).toList)
  val kafka_0_10_1_0_Default = CreateTopic("",1,1,TopicConfigs.configNames(Kafka_0_10_1_0).map(n => TConfig(n,None)).toList)
  val kafka_0_10_1_1_Default = CreateTopic("",1,1,TopicConfigs.configNames(Kafka_0_10_1_1).map(n => TConfig(n,None)).toList)
  val kafka_0_10_2_0_Default = CreateTopic("",1,1,TopicConfigs.configNames(Kafka_0_10_2_0).map(n => TConfig(n,None)).toList)
  val kafka_0_10_2_1_Default = CreateTopic("",1,1,TopicConfigs.configNames(Kafka_0_10_2_1).map(n => TConfig(n,None)).toList)
  val kafka_0_11_0_0_Default = CreateTopic("",1,1,TopicConfigs.configNames(Kafka_0_11_0_0).map(n => TConfig(n,None)).toList)

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

  val defaultAddMultipleTopicsPartitionsForm = Form(
    mapping(
      "topics" -> seq {
        mapping(
          "name" -> nonEmptyText,
          "selected" -> boolean
        )(TopicSelect.apply)(TopicSelect.unapply)
      },
      "brokers" -> seq {
        mapping(
          "id" -> number(min = 0),
          "host" -> nonEmptyText,
          "selected" -> boolean
        )(BrokerSelect.apply)(BrokerSelect.unapply)
      },
      "partitions" -> number(min = 1, max = 10000),
      "readVersions" -> seq {
        mapping(
          "topic" -> nonEmptyText,
          "version" -> number(min = 0)
        )(ReadVersion.apply)(ReadVersion.unapply)
      }
    )(AddMultipleTopicsPartitions.apply)(AddMultipleTopicsPartitions.unapply)
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
    kafkaManager.getClusterContext(clusterName).map { errorOrConfig =>
      errorOrConfig.map { clusterContext =>
        clusterContext.config.version match {
          case Kafka_0_8_1_1 => (defaultCreateForm.fill(kafka_0_8_1_1_Default), clusterContext)
          case Kafka_0_8_2_0 => (defaultCreateForm.fill(kafka_0_8_2_0_Default), clusterContext)
          case Kafka_0_8_2_1 => (defaultCreateForm.fill(kafka_0_8_2_1_Default), clusterContext)
          case Kafka_0_8_2_2 => (defaultCreateForm.fill(kafka_0_8_2_2_Default), clusterContext)
          case Kafka_0_9_0_0 => (defaultCreateForm.fill(kafka_0_9_0_0_Default), clusterContext)
          case Kafka_0_9_0_1 => (defaultCreateForm.fill(kafka_0_9_0_1_Default), clusterContext)
          case Kafka_0_10_0_0 => (defaultCreateForm.fill(kafka_0_10_0_0_Default), clusterContext)
          case Kafka_0_10_0_1 => (defaultCreateForm.fill(kafka_0_10_0_1_Default), clusterContext)
          case Kafka_0_10_1_0 => (defaultCreateForm.fill(kafka_0_10_1_0_Default), clusterContext)
          case Kafka_0_10_1_1 => (defaultCreateForm.fill(kafka_0_10_1_1_Default), clusterContext)
          case Kafka_0_10_2_0 => (defaultCreateForm.fill(kafka_0_10_2_0_Default), clusterContext)
          case Kafka_0_10_2_1 => (defaultCreateForm.fill(kafka_0_10_2_1_Default), clusterContext)
          case Kafka_0_11_0_0 => (defaultCreateForm.fill(kafka_0_11_0_0_Default), clusterContext)

        }
      }
    }
  }

  def topics(c: String) = Action.async {
    kafkaManager.getTopicListExtended(c).map { errorOrTopicList =>
      Ok(views.html.topic.topicList(c,errorOrTopicList))
    }
  }

  def topic(c: String, t: String, force: Boolean) = Action.async {
    val futureErrorOrTopicIdentity = kafkaManager.getTopicIdentity(c,t)
    val futureErrorOrConsumerList = kafkaManager.getConsumersForTopic(c,t)

    futureErrorOrTopicIdentity.zip(futureErrorOrConsumerList).map {case (errorOrTopicIdentity,errorOrConsumerList) =>
      val op = force match {
        case true => ForceRunAssignment
        case _ => RunAssignment
      }
      Ok(views.html.topic.topicView(c,t,errorOrTopicIdentity,errorOrConsumerList, op))
    }
  }

  def createTopic(clusterName: String) = Action.async { implicit request =>
    featureGate(KMTopicManagerFeature) {
      createTopicForm(clusterName).map { errorOrForm =>
        Ok(views.html.topic.createTopic(clusterName, errorOrForm))
      }
    }
  }

  def handleCreateTopic(clusterName: String) = Action.async { implicit request =>
    featureGate(KMTopicManagerFeature) {
      defaultCreateForm.bindFromRequest.fold(
        formWithErrors => {
          kafkaManager.getClusterContext(clusterName).map { clusterContext =>
            BadRequest(views.html.topic.createTopic(clusterName, clusterContext.map(c => (formWithErrors, c))))
          }.recover {
            case t =>
              implicit val clusterFeatures = ClusterFeatures.default
              Ok(views.html.common.resultOfCommand(
                views.html.navigation.clusterMenu(clusterName, "Topic", "Create", menus.clusterMenus(clusterName)),
                models.navigation.BreadCrumbs.withNamedViewAndCluster("Topics", clusterName, "Create Topic"),
                -\/(ApiError(s"Unknown error : ${t.getMessage}")),
                "Create Topic",
                FollowLink("Try again.", routes.Topic.createTopic(clusterName).toString()),
                FollowLink("Try again.", routes.Topic.createTopic(clusterName).toString())
              ))
          }
        },
        ct => {
          val props = new Properties()
          ct.configs.filter(_.value.isDefined).foreach(c => props.setProperty(c.name, c.value.get))
          kafkaManager.createTopic(clusterName, ct.topic, ct.partitions, ct.replication, props).map { errorOrSuccess =>
            implicit val clusterFeatures = errorOrSuccess.toOption.map(_.clusterFeatures).getOrElse(ClusterFeatures.default)
            Ok(views.html.common.resultOfCommand(
              views.html.navigation.clusterMenu(clusterName, "Topic", "Create", menus.clusterMenus(clusterName)),
              models.navigation.BreadCrumbs.withNamedViewAndCluster("Topics", clusterName, "Create Topic"),
              errorOrSuccess,
              "Create Topic",
              FollowLink("Go to topic view.", routes.Topic.topic(clusterName, ct.topic).toString()),
              FollowLink("Try again.", routes.Topic.createTopic(clusterName).toString())
            ))
          }
        }
      )
    }
  }

  def confirmDeleteTopic(clusterName: String, topic: String) = Action.async {
    val futureErrorOrTopicIdentity = kafkaManager.getTopicIdentity(clusterName, topic)
    val futureErrorOrConsumerList = kafkaManager.getConsumersForTopic(clusterName, topic)

    futureErrorOrTopicIdentity.zip(futureErrorOrConsumerList).map {case (errorOrTopicIdentity,errorOrConsumerList) =>
      Ok(views.html.topic.topicDeleteConfirm(clusterName,topic,errorOrTopicIdentity,errorOrConsumerList))
    }
  }

  def handleDeleteTopic(clusterName: String, topic: String) = Action.async { implicit request =>
    featureGate(KMTopicManagerFeature) {
      defaultDeleteForm.bindFromRequest.fold(
        formWithErrors => Future.successful(
          BadRequest(views.html.topic.topicView(
            clusterName,
            topic,
            -\/(ApiError(formWithErrors.error("topic").map(_.toString).getOrElse("Unknown error deleting topic!"))),
            None,
            RunAssignment
          ))
        ),
        deleteTopic => {
          kafkaManager.deleteTopic(clusterName, deleteTopic.topic).map { errorOrSuccess =>
            implicit val clusterFeatures = errorOrSuccess.toOption.map(_.clusterFeatures).getOrElse(ClusterFeatures.default)
            Ok(views.html.common.resultOfCommand(
              views.html.navigation.clusterMenu(clusterName, "Topic", "Topic View", menus.clusterMenus(clusterName)),
              models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View", clusterName, topic, "Delete Topic"),
              errorOrSuccess,
              "Delete Topic",
              FollowLink("Go to topic list.", routes.Topic.topics(clusterName).toString()),
              FollowLink("Try again.", routes.Topic.topic(clusterName, topic).toString())
            ))
          }
        }
      )
    }
  }

  def addPartitions(clusterName: String, topic: String) = Action.async { implicit request =>
    featureGate(KMTopicManagerFeature) {
      val errorOrFormFuture = kafkaManager.getTopicIdentity(clusterName, topic).flatMap { errorOrTopicIdentity =>
        errorOrTopicIdentity.fold(e => Future.successful(-\/(e)), { topicIdentity =>
          kafkaManager.getBrokerList(clusterName).map { errorOrBrokerList =>
            errorOrBrokerList.map { bl =>
              (defaultAddPartitionsForm.fill(AddTopicPartitions(topic, bl.list.map(bi => BrokerSelect.from(bi)), topicIdentity.partitions, topicIdentity.readVersion)),
                bl.clusterContext)
            }
          }
        })
      }
      errorOrFormFuture.map { errorOrForm =>
        Ok(views.html.topic.addPartitions(clusterName, topic, errorOrForm))
      }
    }
  }

  def addPartitionsToMultipleTopics(clusterName: String) = Action.async { implicit request =>
    featureGate(KMTopicManagerFeature) {
      val errorOrFormFuture = kafkaManager.getTopicListExtended(clusterName).flatMap { errorOrTle =>
        errorOrTle.fold(e => Future.successful(-\/(e)), { topicListExtended =>
          kafkaManager.getBrokerList(clusterName).map { errorOrBrokerList =>
            errorOrBrokerList.map { bl =>
              val tl = kafkaManager.topicListSortedByNumPartitions(topicListExtended)
              val topics = tl.map(t => t._1).map(t => TopicSelect.from(t))
              // default value is the largest number of partitions among existing topics with topic identity
              val partitions = tl.head._2.map(_.partitions).getOrElse(0)
              val readVersions = tl.map(t => t._2).flatMap(t => t).map(ti => ReadVersion(ti.topic, ti.readVersion))
              (defaultAddMultipleTopicsPartitionsForm.fill(AddMultipleTopicsPartitions(topics, bl.list.map(bi => BrokerSelect.from(bi)), partitions, readVersions)),
               topicListExtended.clusterContext)
            }
          }
        })
      }
      errorOrFormFuture.map { errorOrForm =>
        Ok(views.html.topic.addPartitionsToMultipleTopics(clusterName, errorOrForm))
      }
    }
  }

  def handleAddPartitions(clusterName: String, topic: String) = Action.async { implicit request =>
    featureGate(KMTopicManagerFeature) {
      defaultAddPartitionsForm.bindFromRequest.fold(
        formWithErrors => {
          kafkaManager.getClusterContext(clusterName).map { clusterContext =>
            BadRequest(views.html.topic.addPartitions(clusterName, topic, clusterContext.map(c => (formWithErrors, c))))
          }.recover {
            case t =>
              implicit val clusterFeatures = ClusterFeatures.default
              Ok(views.html.common.resultOfCommand(
                views.html.navigation.clusterMenu(clusterName, "Topic", "Topic View", menus.clusterMenus(clusterName)),
                models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View", clusterName, topic, "Add Partitions"),
                -\/(ApiError(s"Unknown error : ${t.getMessage}")),
                "Add Partitions",
                FollowLink("Try again.", routes.Topic.addPartitions(clusterName, topic).toString()),
                FollowLink("Try again.", routes.Topic.addPartitions(clusterName, topic).toString())
              ))
          }
        },
        addTopicPartitions => {
          kafkaManager.addTopicPartitions(clusterName, addTopicPartitions.topic, addTopicPartitions.brokers.filter(_.selected).map(_.id), addTopicPartitions.partitions, addTopicPartitions.readVersion).map { errorOrSuccess =>
            implicit val clusterFeatures = errorOrSuccess.toOption.map(_.clusterFeatures).getOrElse(ClusterFeatures.default)
            Ok(views.html.common.resultOfCommand(
              views.html.navigation.clusterMenu(clusterName, "Topic", "Topic View", menus.clusterMenus(clusterName)),
              models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View", clusterName, topic, "Add Partitions"),
              errorOrSuccess,
              "Add Partitions",
              FollowLink("Go to topic view.", routes.Topic.topic(clusterName, addTopicPartitions.topic).toString()),
              FollowLink("Try again.", routes.Topic.addPartitions(clusterName, topic).toString())
            ))
          }
        }
      )
    }
  }

  def handleAddPartitionsToMultipleTopics(clusterName: String) = Action.async { implicit request =>
    featureGate(KMTopicManagerFeature) {
      defaultAddMultipleTopicsPartitionsForm.bindFromRequest.fold(
        formWithErrors => {
          kafkaManager.getClusterContext(clusterName).map { clusterContext =>
            BadRequest(views.html.topic.addPartitionsToMultipleTopics(clusterName, clusterContext.map(c => (formWithErrors, c))))
          }.recover {
            case t =>
              implicit val clusterFeatures = ClusterFeatures.default
              Ok(views.html.common.resultOfCommand(
                views.html.navigation.clusterMenu(clusterName, "Topics", "Add Partitions to Multiple Topics", menus.clusterMenus(clusterName)),
                models.navigation.BreadCrumbs.withNamedViewAndCluster("Topics", clusterName, "Add Partitions to Multiple Topics"),
                -\/(ApiError(s"Unknown error : ${t.getMessage}")),
                "Add Partitions to All Topics",
                FollowLink("Try again.", routes.Topic.addPartitionsToMultipleTopics(clusterName).toString()),
                FollowLink("Try again.", routes.Topic.addPartitionsToMultipleTopics(clusterName).toString())
              ))
          }
        },
        addMultipleTopicsPartitions => {
          val topics = addMultipleTopicsPartitions.topics.filter(_.selected).map(_.name)
          val brokers = addMultipleTopicsPartitions.brokers.filter(_.selected).map(_.id).toSet
          val readVersions = addMultipleTopicsPartitions.readVersions.map { rv => (rv.topic, rv.version)}.toMap
          kafkaManager.addMultipleTopicsPartitions(clusterName, topics, brokers, addMultipleTopicsPartitions.partitions, readVersions).map { errorOrSuccess =>
            implicit val clusterFeatures = errorOrSuccess.toOption.map(_.clusterFeatures).getOrElse(ClusterFeatures.default)
            Ok(views.html.common.resultOfCommand(
              views.html.navigation.clusterMenu(clusterName, "Topics", "Add Partitions to Multiple Topics", menus.clusterMenus(clusterName)),
              models.navigation.BreadCrumbs.withNamedViewAndCluster("Topics", clusterName, "Add Partitions to Multiple Topics"),
              errorOrSuccess,
              "Add Partitions to All Topics",
              FollowLink("Go to topic list.", routes.Topic.topics(clusterName).toString()),
              FollowLink("Try again.", routes.Topic.addPartitionsToMultipleTopics(clusterName).toString())
            ))
          }
        }
      )
    }
  }

  private def updateConfigForm(clusterName: String, ti: TopicIdentity) = {
    kafkaManager.getClusterConfig(clusterName).map { errorOrConfig =>
      errorOrConfig.map { clusterConfig =>
        val defaultConfigMap = clusterConfig.version match {
          case Kafka_0_8_1_1 => TopicConfigs.configNames(Kafka_0_8_1_1).map(n => (n,TConfig(n,None))).toMap
          case Kafka_0_8_2_0 => TopicConfigs.configNames(Kafka_0_8_2_0).map(n => (n,TConfig(n,None))).toMap
          case Kafka_0_8_2_1 => TopicConfigs.configNames(Kafka_0_8_2_1).map(n => (n,TConfig(n,None))).toMap
          case Kafka_0_8_2_2 => TopicConfigs.configNames(Kafka_0_8_2_2).map(n => (n,TConfig(n,None))).toMap
          case Kafka_0_9_0_0 => TopicConfigs.configNames(Kafka_0_9_0_0).map(n => (n,TConfig(n,None))).toMap
          case Kafka_0_9_0_1 => TopicConfigs.configNames(Kafka_0_9_0_1).map(n => (n,TConfig(n,None))).toMap
          case Kafka_0_10_0_0 => TopicConfigs.configNames(Kafka_0_10_0_0).map(n => (n,TConfig(n,None))).toMap
          case Kafka_0_10_0_1 => TopicConfigs.configNames(Kafka_0_10_0_1).map(n => (n,TConfig(n,None))).toMap
          case Kafka_0_10_1_0 => TopicConfigs.configNames(Kafka_0_10_1_0).map(n => (n,TConfig(n,None))).toMap
          case Kafka_0_10_1_1 => TopicConfigs.configNames(Kafka_0_10_1_1).map(n => (n,TConfig(n,None))).toMap
          case Kafka_0_10_2_0 => TopicConfigs.configNames(Kafka_0_10_2_0).map(n => (n,TConfig(n,None))).toMap
          case Kafka_0_10_2_1 => TopicConfigs.configNames(Kafka_0_10_2_1).map(n => (n,TConfig(n,None))).toMap
          case Kafka_0_11_0_0 => TopicConfigs.configNames(Kafka_0_11_0_0).map(n => (n,TConfig(n,None))).toMap
        }
        val combinedMap = defaultConfigMap ++ ti.config.toMap.map(tpl => tpl._1 -> TConfig(tpl._1,Option(tpl._2)))
        (defaultUpdateConfigForm.fill(UpdateTopicConfig(ti.topic,combinedMap.toList.map(_._2),ti.configReadVersion)),
         ti.clusterContext)
      }
    }
  }

  def updateConfig(clusterName: String, topic: String) = Action.async { implicit request =>
    featureGate(KMTopicManagerFeature) {
      val errorOrFormFuture = kafkaManager.getTopicIdentity(clusterName, topic).flatMap { errorOrTopicIdentity =>
        errorOrTopicIdentity.fold(e => Future.successful(-\/(e)), { topicIdentity =>
          updateConfigForm(clusterName, topicIdentity)
        })
      }
      errorOrFormFuture.map { errorOrForm =>
        Ok(views.html.topic.updateConfig(clusterName, topic, errorOrForm))
      }
    }
  }

  def handleUpdateConfig(clusterName: String, topic: String) = Action.async { implicit request =>
    featureGate(KMTopicManagerFeature) {
      defaultUpdateConfigForm.bindFromRequest.fold(
        formWithErrors => {
          kafkaManager.getClusterContext(clusterName).map { clusterContext =>
            BadRequest(views.html.topic.updateConfig(clusterName, topic, clusterContext.map(c => (formWithErrors, c))))
          }.recover {
            case t =>
              implicit val clusterFeatures = ClusterFeatures.default
              Ok(views.html.common.resultOfCommand(
                views.html.navigation.clusterMenu(clusterName, "Topic", "Topic View", menus.clusterMenus(clusterName)),
                models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View", clusterName, topic, "Update Config"),
                -\/(ApiError(s"Unknown error : ${t.getMessage}")),
                "Update Config",
                FollowLink("Try again.", routes.Topic.updateConfig(clusterName, topic).toString()),
                FollowLink("Try again.", routes.Topic.updateConfig(clusterName, topic).toString())
              ))
          }
        },
        updateTopicConfig => {
          val props = new Properties()
          updateTopicConfig.configs.filter(_.value.isDefined).foreach(c => props.setProperty(c.name, c.value.get))
          kafkaManager.updateTopicConfig(clusterName, updateTopicConfig.topic, props, updateTopicConfig.readVersion).map { errorOrSuccess =>
            implicit val clusterFeatures = errorOrSuccess.toOption.map(_.clusterFeatures).getOrElse(ClusterFeatures.default)
            Ok(views.html.common.resultOfCommand(
              views.html.navigation.clusterMenu(clusterName, "Topic", "Topic View", menus.clusterMenus(clusterName)),
              models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View", clusterName, topic, "Update Config"),
              errorOrSuccess,
              "Update Config",
              FollowLink("Go to topic view.", routes.Topic.topic(clusterName, updateTopicConfig.topic).toString()),
              FollowLink("Try again.", routes.Topic.updateConfig(clusterName, topic).toString())
            ))
          }
        }
      )
    }
  }
}
