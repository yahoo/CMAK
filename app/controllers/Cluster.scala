/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import features.{ApplicationFeatures, KMClusterManagerFeature}
import kafka.manager.model._
import kafka.manager.ApiError
import models.FollowLink
import models.form._
import models.navigation.Menus
import play.api.data.Form
import play.api.data.Forms._
import play.api.data.validation.{Constraint, Invalid, Valid}
import play.api.data.validation.Constraints._
import play.api.i18n.{I18nSupport, MessagesApi}
import play.api.mvc._

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scalaz.{-\/, \/-}

/**
 * @author hiral
 */
class Cluster (val messagesApi: MessagesApi, val kafkaManagerContext: KafkaManagerContext)
              (implicit af: ApplicationFeatures, menus: Menus) extends Controller with I18nSupport {
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = kafkaManagerContext.getKafkaManager
  private[this] val defaultTuning = kafkaManager.defaultTuning

  val validateName : Constraint[String] = Constraint("validate name") { name =>
    Try {
      ClusterConfig.validateName(name)
    } match {
      case Failure(t) => Invalid(t.getMessage)
      case Success(_) => Valid
    }
  }

  val validateZkHosts : Constraint[String] = Constraint("validate zookeeper hosts") { zkHosts =>
    Try {
      ClusterConfig.validateZkHosts(zkHosts)
    } match {
      case Failure(t) => Invalid(t.getMessage)
      case Success(_) => Valid
    }
  }

  val validateOperation : Constraint[String] = Constraint("validate operation value") {
    case "Enable" => Valid
    case "Disable" => Valid
    case "Delete" => Valid
    case "Update" => Valid
    case any: Any => Invalid(s"Invalid operation value: $any")
  }

  val validateKafkaVersion: Constraint[String] = Constraint("validate kafka version") { version =>
    Try {
      KafkaVersion(version)
    } match {
      case Failure(t) => Invalid(t.getMessage)
      case Success(_) => Valid
    }
  }

  val validateSecurityProtocol: Constraint[String] = Constraint("validate security protocol") { string =>
    Try {
      SecurityProtocol(string)
    } match {
      case Failure(t) => Invalid(t.getMessage)
      case Success(_) => Valid
    }
  }

  val clusterConfigForm = Form(
    mapping(
      "name" -> nonEmptyText.verifying(maxLength(250), validateName)
      , "kafkaVersion" -> nonEmptyText.verifying(validateKafkaVersion)
      , "zkHosts" -> nonEmptyText.verifying(validateZkHosts)
      , "zkMaxRetry" -> ignored(100 : Int)
      , "jmxEnabled" -> boolean
      , "jmxUser" -> optional(text)
      , "jmxPass" -> optional(text)
      , "jmxSsl" -> boolean
      , "pollConsumers" -> boolean
      , "filterConsumers" -> boolean
      , "logkafkaEnabled" -> boolean
      , "activeOffsetCacheEnabled" -> boolean
      , "displaySizeEnabled" -> boolean
      , "tuning" -> optional(
        mapping(
          "brokerViewUpdatePeriodSeconds" -> optional(number(10, 1000))
          , "clusterManagerThreadPoolSize" -> optional(number(2, 1000))
          , "clusterManagerThreadPoolQueueSize" -> optional(number(10, 10000))
          , "kafkaCommandThreadPoolSize" -> optional(number(2, 1000))
          , "kafkaCommandThreadPoolQueueSize" -> optional(number(10, 10000))
          , "logkafkaCommandThreadPoolSize" -> optional(number(2, 1000))
          , "logkafkaCommandThreadPoolQueueSize" -> optional(number(10, 10000))
          , "logkafkaUpdatePeriodSeconds" -> optional(number(10, 1000))
          , "partitionOffsetCacheTimeoutSecs" -> optional(number(5, 100))
          , "brokerViewThreadPoolSize" -> optional(number(2, 1000))
          , "brokerViewThreadPoolQueueSize" -> optional(number(10, 10000))
          , "offsetCacheThreadPoolSize" -> optional(number(2, 1000))
          , "offsetCacheThreadPoolQueueSize" -> optional(number(10, 10000))
          , "kafkaAdminClientThreadPoolSize" -> optional(number(2, 1000))
          , "kafkaAdminClientThreadPoolQueueSize" -> optional(number(10, 10000))
        )(ClusterTuning.apply)(ClusterTuning.unapply)
      )
      , "securityProtocol" -> nonEmptyText.verifying(validateSecurityProtocol)
    )(ClusterConfig.apply)(ClusterConfig.customUnapply)
  )

  val updateForm = Form(
    mapping(
      "operation" -> nonEmptyText.verifying(validateOperation),
      "name" -> nonEmptyText.verifying(maxLength(250), validateName),
      "kafkaVersion" -> nonEmptyText.verifying(validateKafkaVersion),
      "zkHosts" -> nonEmptyText.verifying(validateZkHosts),
      "zkMaxRetry" -> ignored(100 : Int),
      "jmxEnabled" -> boolean,
      "jmxUser" -> optional(text),
      "jmxPass" -> optional(text),
      "jmxSsl" -> boolean,
      "pollConsumers" -> boolean,
      "filterConsumers" -> boolean,
      "logkafkaEnabled" -> boolean,
      "activeOffsetCacheEnabled" -> boolean,
      "displaySizeEnabled" -> boolean,
      "tuning" -> optional(
        mapping(
          "brokerViewUpdatePeriodSeconds" -> optional(number(10, 1000))
          , "clusterManagerThreadPoolSize" -> optional(number(2, 1000))
          , "clusterManagerThreadPoolQueueSize" -> optional(number(10, 10000))
          , "kafkaCommandThreadPoolSize" -> optional(number(2, 1000))
          , "kafkaCommandThreadPoolQueueSize" -> optional(number(10, 10000))
          , "logkafkaCommandThreadPoolSize" -> optional(number(2, 1000))
          , "logkafkaCommandThreadPoolQueueSize" -> optional(number(10, 10000))
          , "logkafkaUpdatePeriodSeconds" -> optional(number(10, 1000))
          , "partitionOffsetCacheTimeoutSecs" -> optional(number(5, 100))
          , "brokerViewThreadPoolSize" -> optional(number(2, 1000))
          , "brokerViewThreadPoolQueueSize" -> optional(number(10, 10000))
          , "offsetCacheThreadPoolSize" -> optional(number(2, 1000))
          , "offsetCacheThreadPoolQueueSize" -> optional(number(10, 10000))
          , "kafkaAdminClientThreadPoolSize" -> optional(number(2, 1000))
          , "kafkaAdminClientThreadPoolQueueSize" -> optional(number(10, 10000))
        )(ClusterTuning.apply)(ClusterTuning.unapply)
      )
      , "securityProtocol" -> nonEmptyText.verifying(validateSecurityProtocol)
    )(ClusterOperation.apply)(ClusterOperation.customUnapply)
  )

  private[this] val defaultClusterConfig : ClusterConfig = {
    ClusterConfig(
      ""
      ,CuratorConfig("")
      ,false
      ,KafkaVersion.supportedVersions.values.toList.sortBy(_.toString).last
      ,false
      ,None
      ,None
      ,false
      ,false
      ,false
      ,false
      ,false
      ,false
      ,Option(defaultTuning)
      ,PLAINTEXT
    )
  }

  def cluster(c: String) = Action.async {
    kafkaManager.getClusterView(c).map { errorOrClusterView =>
      Ok(views.html.cluster.clusterView(c,errorOrClusterView))
    }
  }

  def brokers(c: String) = Action.async {
    kafkaManager.getBrokerList(c).map { errorOrBrokerList =>
      Ok(views.html.broker.brokerList(c,errorOrBrokerList))
    }
  }

  def broker(c: String, b: Int) = Action.async {
    kafkaManager.getBrokerView(c,b).map { errorOrBrokerView =>
      Ok(views.html.broker.brokerView(c,b,errorOrBrokerView))
    }
  }

  def addCluster = Action.async { implicit request =>
    featureGate(KMClusterManagerFeature) {
      Future.successful(Ok(views.html.cluster.addCluster(clusterConfigForm.fill(defaultClusterConfig))))
    }
  }

  def updateCluster(c: String) = Action.async { implicit request =>
    featureGate(KMClusterManagerFeature) {
      kafkaManager.getClusterConfig(c).map { errorOrClusterConfig =>
        Ok(views.html.cluster.updateCluster(c,errorOrClusterConfig.map { cc =>
          updateForm.fill(ClusterOperation.apply(
            Update.toString,
            cc.name,
            cc.version.toString,
            cc.curatorConfig.zkConnect,
            cc.curatorConfig.zkMaxRetry,
            cc.jmxEnabled,
            cc.jmxUser,
            cc.jmxPass,
            cc.jmxSsl,
            cc.pollConsumers,
            cc.filterConsumers,
            cc.logkafkaEnabled,
            cc.activeOffsetCacheEnabled,
            cc.displaySizeEnabled,
            cc.tuning,
            cc.securityProtocol.stringId
          ))
        }))
      }
    }

  }

  def handleAddCluster = Action.async { implicit request =>
    featureGate(KMClusterManagerFeature) {
      clusterConfigForm.bindFromRequest.fold(
        formWithErrors => Future.successful(BadRequest(views.html.cluster.addCluster(formWithErrors))),
        clusterConfig => {
          kafkaManager.addCluster(clusterConfig.name,
            clusterConfig.version.toString,
            clusterConfig.curatorConfig.zkConnect,
            clusterConfig.jmxEnabled,
            clusterConfig.jmxUser,
            clusterConfig.jmxPass,
            clusterConfig.jmxSsl,
            clusterConfig.pollConsumers,
            clusterConfig.filterConsumers,
            clusterConfig.tuning,
            clusterConfig.securityProtocol.stringId,
            clusterConfig.logkafkaEnabled,
            clusterConfig.activeOffsetCacheEnabled,
            clusterConfig.displaySizeEnabled
          ).map { errorOrSuccess =>
            Ok(views.html.common.resultOfCommand(
              views.html.navigation.defaultMenu(),
              models.navigation.BreadCrumbs.withView("Add Cluster"),
              errorOrSuccess,
              "Add Cluster",
              FollowLink("Go to cluster view.",routes.Cluster.cluster(clusterConfig.name).toString()),
              FollowLink("Try again.",routes.Cluster.addCluster().toString())
            ))
          }
        }
      )
    }
  }

  def handleUpdateCluster(c: String) = Action.async { implicit request =>
    featureGate(KMClusterManagerFeature) {
      updateForm.bindFromRequest.fold(
        formWithErrors => Future.successful(BadRequest(views.html.cluster.updateCluster(c, \/-(formWithErrors)))),
        clusterOperation => clusterOperation.op match {
          case Enable =>
            kafkaManager.enableCluster(c).map { errorOrSuccess =>
              Ok(views.html.common.resultOfCommand(
                views.html.navigation.defaultMenu(),
                models.navigation.BreadCrumbs.withViewAndCluster("Enable Cluster", c),
                errorOrSuccess,
                "Enable Cluster",
                FollowLink("Go to cluster list.", routes.Application.index().toString()),
                FollowLink("Back to cluster list.", routes.Application.index().toString())
              ))
            }
          case Disable =>
            kafkaManager.disableCluster(c).map { errorOrSuccess =>
              Ok(views.html.common.resultOfCommand(
                views.html.navigation.defaultMenu(),
                models.navigation.BreadCrumbs.withViewAndCluster("Disable Cluster", c),
                errorOrSuccess,
                "Disable Cluster",
                FollowLink("Back to cluster list.", routes.Application.index().toString()),
                FollowLink("Back to cluster list.", routes.Application.index().toString())
              ))
            }
          case Delete =>
            kafkaManager.deleteCluster(c).map { errorOrSuccess =>
              Ok(views.html.common.resultOfCommand(
                views.html.navigation.defaultMenu(),
                models.navigation.BreadCrumbs.withViewAndCluster("Delete Cluster", c),
                errorOrSuccess,
                "Delete Cluster",
                FollowLink("Back to cluster list.", routes.Application.index().toString()),
                FollowLink("Back to cluster list.", routes.Application.index().toString())
              ))
            }
          case Update =>
            kafkaManager.updateCluster(
              clusterOperation.clusterConfig.name,
              clusterOperation.clusterConfig.version.toString,
              clusterOperation.clusterConfig.curatorConfig.zkConnect,
              clusterOperation.clusterConfig.jmxEnabled,
              clusterOperation.clusterConfig.jmxUser,
              clusterOperation.clusterConfig.jmxPass,
              clusterOperation.clusterConfig.jmxSsl,
              clusterOperation.clusterConfig.pollConsumers,
              clusterOperation.clusterConfig.filterConsumers,
              clusterOperation.clusterConfig.tuning,
              clusterOperation.clusterConfig.securityProtocol.stringId,
              clusterOperation.clusterConfig.logkafkaEnabled,
              clusterOperation.clusterConfig.activeOffsetCacheEnabled,
              clusterOperation.clusterConfig.displaySizeEnabled
            ).map { errorOrSuccess =>
              Ok(views.html.common.resultOfCommand(
                views.html.navigation.defaultMenu(),
                models.navigation.BreadCrumbs.withViewAndCluster("Update Cluster", c),
                errorOrSuccess,
                "Update Cluster",
                FollowLink("Go to cluster view.", routes.Cluster.cluster(clusterOperation.clusterConfig.name).toString()),
                FollowLink("Try again.", routes.Cluster.updateCluster(c).toString())
              ))
            }
          case Unknown(opString) =>
            Future.successful(Ok(views.html.common.resultOfCommand(
              views.html.navigation.defaultMenu(),
              models.navigation.BreadCrumbs.withViewAndCluster("Unknown Cluster Operation", c),
              -\/(ApiError(s"Unknown operation $opString")),
              "Unknown Cluster Operation",
              FollowLink("Back to cluster list.", routes.Application.index().toString()),
              FollowLink("Back to cluster list.", routes.Application.index().toString())
            )))
        }
      )
    }
  }
}
