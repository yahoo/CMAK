/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import java.util.Properties

import _root_.features.ApplicationFeatures
import kafka.manager._
import kafka.manager.features.KMLogKafkaFeature
import kafka.manager.model.ActorModel.LogkafkaIdentity
import kafka.manager.model._
import kafka.manager.utils.LogkafkaNewConfigs
import models.FollowLink
import models.form._
import models.navigation.Menus
import play.api.data.Form
import play.api.data.Forms._
import play.api.data.validation.Constraints._
import play.api.data.validation.{Constraint, Invalid, Valid}
import play.api.i18n.I18nSupport
import play.api.mvc._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scalaz.{-\/, \/-}

/**
 * @author hiral
 */
class Logkafka (val cc: ControllerComponents, val kafkaManagerContext: KafkaManagerContext)
               (implicit af: ApplicationFeatures, menus: Menus, ec:ExecutionContext)  extends AbstractController(cc) with I18nSupport {

  implicit private[this] val kafkaManager = kafkaManagerContext.getKafkaManager

  val validateLogkafkaId: Constraint[String] = Constraint("validate logkafka id") { id =>
    Try {
      kafka.manager.utils.Logkafka.validateLogkafkaId(id)
    } match {
      case Failure(t) => Invalid(t.getMessage)
      case Success(_) => Valid
    }
  }

  val validatePath: Constraint[String] = Constraint("validate path") { path =>
    Try {
      kafka.manager.utils.Logkafka.validatePath(path)
    } match {
      case Failure(t) => Invalid(t.getMessage)
      case Success(_) => Valid
    }
  }
  
  val kafka_0_8_1_1_Default = CreateLogkafka("","",
      LogkafkaNewConfigs.configMaps(Kafka_0_8_1_1).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_0_8_2_0_Default = CreateLogkafka("","",
      LogkafkaNewConfigs.configMaps(Kafka_0_8_2_0).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_0_8_2_1_Default = CreateLogkafka("","",
      LogkafkaNewConfigs.configMaps(Kafka_0_8_2_1).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_0_8_2_2_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_0_8_2_2).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_0_9_0_0_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_0_9_0_0).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_0_9_0_1_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_0_9_0_1).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_0_10_0_0_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_0_10_0_0).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_0_10_0_1_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_0_10_0_1).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_0_10_1_0_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_0_10_1_0).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_0_10_1_1_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_0_10_1_1).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_0_10_2_0_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_0_10_2_0).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_0_10_2_1_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_0_10_2_1).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_0_11_0_0_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_0_11_0_0).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_0_11_0_2_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_0_11_0_2).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_1_0_0_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_1_0_0).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_1_0_1_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_1_0_1).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_1_1_0_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_1_1_0).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_1_1_1_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_1_1_1).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_2_0_0_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_2_0_0).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_2_1_0_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_2_1_0).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_2_1_1_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_2_1_1).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_2_2_0_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_2_2_0).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_2_4_0_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_2_4_0).map{case(k,v) => LKConfig(k,Some(v))}.toList)
  val kafka_2_4_1_Default = CreateLogkafka("","",
    LogkafkaNewConfigs.configMaps(Kafka_2_4_1).map{case(k,v) => LKConfig(k,Some(v))}.toList)

  val defaultCreateForm = Form(
    mapping(
      "logkafka_id" -> nonEmptyText.verifying(maxLength(250), validateLogkafkaId),
      "log_path" -> nonEmptyText.verifying(maxLength(250), validatePath),
      "configs" -> list(
        mapping(
          "name" -> nonEmptyText,
          "value" -> optional(text)
        )(LKConfig.apply)(LKConfig.unapply)
      )
    )(CreateLogkafka.apply)(CreateLogkafka.unapply)
  )
  
  val defaultDeleteForm = Form(
    mapping(
      "logkafka_id" -> nonEmptyText.verifying(maxLength(250), validateLogkafkaId),
      "log_path" -> nonEmptyText.verifying(maxLength(250), validatePath)
    )(DeleteLogkafka.apply)(DeleteLogkafka.unapply)
  )

  val defaultUpdateConfigForm = Form(
    mapping(
      "logkafka_id" -> nonEmptyText.verifying(maxLength(250), validateLogkafkaId),
      "log_path" -> nonEmptyText.verifying(maxLength(250), validatePath),
      "configs" -> list(
        mapping(
          "name" -> nonEmptyText,
          "value" -> optional(text)
        )(LKConfig.apply)(LKConfig.unapply)
      )
    )(UpdateLogkafkaConfig.apply)(UpdateLogkafkaConfig.unapply)
  )

  private def createLogkafkaForm(clusterName: String) = {
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
          case Kafka_0_11_0_2 => (defaultCreateForm.fill(kafka_0_11_0_2_Default), clusterContext)
          case Kafka_1_0_0 => (defaultCreateForm.fill(kafka_1_0_0_Default), clusterContext)
          case Kafka_1_0_1 => (defaultCreateForm.fill(kafka_1_0_1_Default), clusterContext)
          case Kafka_1_1_0 => (defaultCreateForm.fill(kafka_1_1_0_Default), clusterContext)
          case Kafka_1_1_1 => (defaultCreateForm.fill(kafka_1_1_1_Default), clusterContext)
          case Kafka_2_0_0 => (defaultCreateForm.fill(kafka_2_0_0_Default), clusterContext)
          case Kafka_2_1_0 => (defaultCreateForm.fill(kafka_2_1_0_Default), clusterContext)
          case Kafka_2_1_1 => (defaultCreateForm.fill(kafka_2_1_1_Default), clusterContext)
          case Kafka_2_2_0 => (defaultCreateForm.fill(kafka_2_2_0_Default), clusterContext)
          case Kafka_2_4_0 => (defaultCreateForm.fill(kafka_2_4_0_Default), clusterContext)
          case Kafka_2_4_1 => (defaultCreateForm.fill(kafka_2_4_1_Default), clusterContext)
        }
      }
    }
  }

  def logkafkas(c: String) = Action.async { implicit request:RequestHeader =>
    clusterFeatureGate(c, KMLogKafkaFeature) { clusterContext =>
      kafkaManager.getLogkafkaListExtended(c).map { errorOrLogkafkaList =>
        Ok(views.html.logkafka.logkafkaList(c, errorOrLogkafkaList.map( lkle => (lkle, clusterContext)))).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
      }
    }
  }

  def logkafka(c: String, h: String, l:String) = Action.async { implicit request:RequestHeader =>
    clusterFeatureGate(c, KMLogKafkaFeature) { clusterContext =>
      kafkaManager.getLogkafkaIdentity(c, h).map { errorOrLogkafkaIdentity =>
        Ok(views.html.logkafka.logkafkaView(c, h, l, errorOrLogkafkaIdentity.map( lki => (lki, clusterContext)))).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
      }
    }
  }

  def createLogkafka(clusterName: String) = Action.async { implicit request:RequestHeader =>
    clusterFeatureGate(clusterName, KMLogKafkaFeature) { clusterContext =>
      createLogkafkaForm(clusterName).map { errorOrForm =>
        Ok(views.html.logkafka.createLogkafka(clusterName, errorOrForm)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
      }
    }
  }

  def handleCreateLogkafka(clusterName: String) = Action.async { implicit request:Request[AnyContent] =>
    clusterFeatureGate(clusterName, KMLogKafkaFeature) { clusterContext =>
      implicit val clusterFeatures = clusterContext.clusterFeatures
      defaultCreateForm.bindFromRequest.fold(
        formWithErrors => {
            Future.successful(BadRequest(views.html.logkafka.createLogkafka(clusterName, \/-((formWithErrors, clusterContext)))))
        },
        cl => {
          val props = new Properties()
          cl.configs.filter(_.value.isDefined).foreach(c => props.setProperty(c.name, c.value.get))
          kafkaManager.createLogkafka(clusterName, cl.logkafka_id, cl.log_path, props).map { errorOrSuccess =>
            Ok(views.html.common.resultOfCommand(
              views.html.navigation.clusterMenu(clusterName, "Logkafka", "Create", menus.clusterMenus(clusterName)),
              models.navigation.BreadCrumbs.withNamedViewAndCluster("Logkafkas", clusterName, "Create Logkafka"),
              errorOrSuccess,
              "Create Logkafka",
              FollowLink("Go to logkafka id view.", routes.Logkafka.logkafka(clusterName, cl.logkafka_id, cl.log_path).toString()),
              FollowLink("Try again.", routes.Logkafka.createLogkafka(clusterName).toString())
            )).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
          }
        }
      )
    }
  }

  def handleDeleteLogkafka(clusterName: String, logkafka_id: String, log_path: String) = Action.async { implicit request:Request[AnyContent] =>
    clusterFeatureGate(clusterName, KMLogKafkaFeature) { clusterContext =>
      implicit val clusterFeatures = clusterContext.clusterFeatures
      defaultDeleteForm.bindFromRequest.fold(
        formWithErrors => Future.successful(
          BadRequest(views.html.logkafka.logkafkaView(
            clusterName,
            logkafka_id,
            log_path,
            -\/(ApiError(formWithErrors.error("logkafka").map(_.toString).getOrElse("Unknown error deleting logkafka!")))))),
        deleteLogkafka => {
          kafkaManager.deleteLogkafka(clusterName, deleteLogkafka.logkafka_id, deleteLogkafka.log_path).map { errorOrSuccess =>
            Ok(views.html.common.resultOfCommand(
              views.html.navigation.clusterMenu(clusterName, "Logkafka", "Logkafka View", menus.clusterMenus(clusterName)),
              models.navigation.BreadCrumbs.withNamedViewAndClusterAndLogkafka("Logkafka View", clusterName, logkafka_id, log_path, "Delete Logkafka"),
              errorOrSuccess,
              "Delete Logkafka",
              FollowLink("Go to logkafka list.", routes.Logkafka.logkafkas(clusterName).toString()),
              FollowLink("Try again.", routes.Logkafka.logkafka(clusterName, logkafka_id, log_path).toString())
            )).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
          }
        }
      )
    }
  }

  private def updateConfigForm(clusterContext: ClusterContext, log_path: String, li: LogkafkaIdentity) = {
    val defaultConfigMap = clusterContext.config.version match {
      case Kafka_0_8_1_1 => LogkafkaNewConfigs.configNames(Kafka_0_8_1_1).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_0_8_2_0 => LogkafkaNewConfigs.configNames(Kafka_0_8_2_0).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_0_8_2_1 => LogkafkaNewConfigs.configNames(Kafka_0_8_2_1).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_0_8_2_2 => LogkafkaNewConfigs.configNames(Kafka_0_8_2_2).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_0_9_0_0 => LogkafkaNewConfigs.configNames(Kafka_0_9_0_0).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_0_9_0_1 => LogkafkaNewConfigs.configNames(Kafka_0_9_0_1).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_0_10_0_0 => LogkafkaNewConfigs.configNames(Kafka_0_10_0_0).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_0_10_0_1 => LogkafkaNewConfigs.configNames(Kafka_0_10_0_1).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_0_10_1_0 => LogkafkaNewConfigs.configNames(Kafka_0_10_1_0).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_0_10_1_1 => LogkafkaNewConfigs.configNames(Kafka_0_10_1_1).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_0_10_2_0 => LogkafkaNewConfigs.configNames(Kafka_0_10_2_0).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_0_10_2_1 => LogkafkaNewConfigs.configNames(Kafka_0_10_2_1).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_0_11_0_0 => LogkafkaNewConfigs.configNames(Kafka_0_11_0_0).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_0_11_0_2 => LogkafkaNewConfigs.configNames(Kafka_0_11_0_2).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_1_0_0 => LogkafkaNewConfigs.configNames(Kafka_1_0_0).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_1_0_1 => LogkafkaNewConfigs.configNames(Kafka_1_0_1).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_1_1_0 => LogkafkaNewConfigs.configNames(Kafka_1_1_0).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_1_1_1 => LogkafkaNewConfigs.configNames(Kafka_1_1_1).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_2_0_0 => LogkafkaNewConfigs.configNames(Kafka_2_0_0).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_2_1_0 => LogkafkaNewConfigs.configNames(Kafka_2_1_0).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_2_1_1 => LogkafkaNewConfigs.configNames(Kafka_2_1_1).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_2_2_0 => LogkafkaNewConfigs.configNames(Kafka_2_2_0).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_2_4_0 => LogkafkaNewConfigs.configNames(Kafka_2_4_0).map(n => (n,LKConfig(n,None))).toMap
      case Kafka_2_4_1 => LogkafkaNewConfigs.configNames(Kafka_2_4_1).map(n => (n,LKConfig(n,None))).toMap
    }
    val identityOption = li.identityMap.get(log_path)
    if (identityOption.isDefined) {
      val configOption = identityOption.get._1
      if (configOption.isDefined) {
        val config: Map[String, String] = configOption.get
        val combinedMap = defaultConfigMap ++ config.map(tpl => tpl._1 -> LKConfig(tpl._1,Option(tpl._2)))
        defaultUpdateConfigForm.fill(UpdateLogkafkaConfig(li.logkafka_id,log_path,combinedMap.toList.map(_._2)))
      } else {
        defaultUpdateConfigForm.fill(UpdateLogkafkaConfig(li.logkafka_id,log_path,List(LKConfig("",None))))
      }
    } else {
      defaultUpdateConfigForm.fill(UpdateLogkafkaConfig(li.logkafka_id,log_path,List(LKConfig("",None))))
    }
  }

  def updateConfig(clusterName: String, logkafka_id: String, log_path: String) = Action.async { implicit request:RequestHeader =>
    clusterFeatureGate(clusterName, KMLogKafkaFeature) { clusterContext =>
      val errorOrFormFuture = kafkaManager.getLogkafkaIdentity(clusterName, logkafka_id).map(
          _.map(lki => (updateConfigForm(clusterContext, log_path, lki), clusterContext))
      )
      errorOrFormFuture.map { errorOrForm =>
        Ok(views.html.logkafka.updateConfig(clusterName, logkafka_id, log_path, errorOrForm)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
      }
    }
  }

  def handleUpdateConfig(clusterName: String, logkafka_id: String, log_path: String) = Action.async { implicit request:Request[AnyContent] =>
    clusterFeatureGate(clusterName, KMLogKafkaFeature) { clusterContext =>
      implicit val clusterFeatures = clusterContext.clusterFeatures
      defaultUpdateConfigForm.bindFromRequest.fold(
        formWithErrors => Future.successful(BadRequest(views.html.logkafka.updateConfig(clusterName, logkafka_id, log_path, \/-((formWithErrors, clusterContext))))),
        updateLogkafkaConfig => {
          val props = new Properties()
          updateLogkafkaConfig.configs.filter(_.value.isDefined).foreach(c => props.setProperty(c.name, c.value.get))
          kafkaManager.updateLogkafkaConfig(clusterName, updateLogkafkaConfig.logkafka_id, updateLogkafkaConfig.log_path, props).map { errorOrSuccess =>
            Ok(views.html.common.resultOfCommand(
              views.html.navigation.clusterMenu(clusterName, "Logkafka", "Logkafka View", menus.clusterMenus(clusterName)),
              models.navigation.BreadCrumbs.withNamedViewAndClusterAndLogkafka("Logkafka View", clusterName, logkafka_id, log_path, "Update Config"),
              errorOrSuccess,
              "Update Config",
              FollowLink("Go to logkafka view.", routes.Logkafka.logkafka(clusterName, updateLogkafkaConfig.logkafka_id, updateLogkafkaConfig.log_path).toString()),
              FollowLink("Try again.", routes.Logkafka.updateConfig(clusterName, logkafka_id, log_path).toString())
            )).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
          }
        }
      )
    }
  }

  def handleEnableConfig(clusterName: String, logkafka_id: String, log_path: String) = Action.async { implicit request:RequestHeader =>
    clusterFeatureGate(clusterName, KMLogKafkaFeature) { clusterContext =>
      implicit val clusterFeatures = clusterContext.clusterFeatures
      val props = new Properties();
      props.put("valid", true.toString);
      kafkaManager.updateLogkafkaConfig(clusterName, logkafka_id, log_path, props, false).map { errorOrSuccess =>
        Ok(views.html.common.resultOfCommand(
          views.html.navigation.clusterMenu(clusterName, "Logkafka", "Logkafka View", menus.clusterMenus(clusterName)),
          models.navigation.BreadCrumbs.withNamedViewAndClusterAndLogkafka("Logkafka View", clusterName, logkafka_id, log_path, "Update Config"),
          errorOrSuccess,
          "Enable Config",
          FollowLink("Go to logkafka view.", routes.Logkafka.logkafka(clusterName, logkafka_id, log_path).toString()),
          FollowLink("Try again.", routes.Logkafka.updateConfig(clusterName, logkafka_id, log_path).toString())
        )).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
      }
    }
  }

  def handleDisableConfig(clusterName: String, logkafka_id: String, log_path: String) = Action.async { implicit request:RequestHeader =>
    clusterFeatureGate(clusterName, KMLogKafkaFeature) { clusterContext =>
      implicit val clusterFeatures = clusterContext.clusterFeatures
      val props = new Properties();
      props.put("valid", false.toString);
      kafkaManager.updateLogkafkaConfig(clusterName, logkafka_id, log_path, props, false).map { errorOrSuccess =>
        Ok(views.html.common.resultOfCommand(
          views.html.navigation.clusterMenu(clusterName, "Logkafka", "Logkafka View", menus.clusterMenus(clusterName)),
          models.navigation.BreadCrumbs.withNamedViewAndClusterAndLogkafka("Logkafka View", clusterName, logkafka_id, log_path, "Update Config"),
          errorOrSuccess,
          "Disable Config",
          FollowLink("Go to logkafka view.", routes.Logkafka.logkafka(clusterName, logkafka_id, log_path).toString()),
          FollowLink("Try again.", routes.Logkafka.updateConfig(clusterName, logkafka_id, log_path).toString())
        )).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
      }
    }
  }
}
