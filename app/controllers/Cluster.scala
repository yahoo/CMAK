/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import features.{KMClusterManagerFeature, ApplicationFeatures}
import kafka.manager.{SchedulerConfig, KafkaVersion, ApiError, ClusterConfig}
import models.FollowLink
import models.form._
import play.api.data.Form
import play.api.data.Forms._
import play.api.data.validation.{Valid, Invalid, Constraint}
import play.api.data.validation.Constraints._
import play.api.mvc._

import scala.concurrent.Future
import scala.util.{Success, Failure, Try}
import scalaz.{-\/, \/-}

/**
 * @author hiral
 */
object Cluster extends Controller {
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = KafkaManagerContext.getKafkaManager
  private[this] implicit val af: ApplicationFeatures = ApplicationFeatures.features

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

  val clusterConfigForm = Form(
    mapping(
      "name" -> nonEmptyText.verifying(maxLength(250), validateName),
      "kafkaVersion" -> nonEmptyText.verifying(validateKafkaVersion),
      "zkHosts" -> nonEmptyText.verifying(validateZkHosts),
      "zkMaxRetry" -> ignored(100 : Int),
      "jmxEnabled" -> boolean,
      "jmxUser" -> optional(text),
      "jmxPass" -> optional(text),
      "pollConsumers" -> boolean,
      "filterConsumers" -> boolean,
      "logkafkaEnabled" -> boolean,
      "activeOffsetCacheEnabled" -> boolean,
      "displaySizeEnabled" -> boolean
    )(ClusterConfig.apply)(ClusterConfig.customUnapply)
  )

  val schedulerConfigForm = Form(
    mapping(
      "name" -> nonEmptyText.verifying(maxLength(250), validateName),
      "kafkaVersion" -> nonEmptyText.verifying(validateKafkaVersion),
      "apiUrl" -> nonEmptyText.verifying(validateZkHosts),
      "zkHosts" -> nonEmptyText.verifying(validateZkHosts),
      "zkMaxRetry" -> ignored(100 : Int),
      "jmxEnabled" -> boolean
    )(SchedulerConfig.apply)(SchedulerConfig.customUnapply)
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
      "pollConsumers" -> boolean,
      "filterConsumers" -> boolean,
      "logkafkaEnabled" -> boolean,
      "activeOffsetCacheEnabled" -> boolean,
      "displaySizeEnabled" -> boolean
    )(ClusterOperation.apply)(ClusterOperation.customUnapply)
  )

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
      Future.successful(Ok(views.html.cluster.addCluster(clusterConfigForm)))
    }
  }

  def addScheduler = Action.async { implicit request =>
    Future.successful(Ok(scheduler.views.html.scheduler.addScheduler(schedulerConfigForm)))
  }

  def handleAddScheduler = Action.async { implicit request =>
    schedulerConfigForm.bindFromRequest.fold(
      formWithErrors => Future.successful(BadRequest(scheduler.views.html.scheduler.addScheduler(formWithErrors))),
      schedulerConfig => {
        kafkaManager.addScheduler(schedulerConfig.name, schedulerConfig.version.toString, schedulerConfig.apiUrl, schedulerConfig.curatorConfig.zkConnect, jmxEnabled = true).map { errorOrSuccess =>
          Ok(views.html.common.resultOfCommand(
            views.html.navigation.defaultMenu(),
            models.navigation.BreadCrumbs.withView("Add Scheduler"),
            errorOrSuccess,
            "Add Scheduler",
            FollowLink("Go to scheduler view.",scheduler.controllers.routes.SchedulerApplication.getScheduler(schedulerConfig.name).toString()),
            FollowLink("Try again.",routes.Cluster.addScheduler().toString())
          ))
        }
      }
    )
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
            cc.pollConsumers,
            cc.filterConsumers,
            cc.logkafkaEnabled,
            cc.activeOffsetCacheEnabled,
            cc.displaySizeEnabled))
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
            clusterConfig.pollConsumers,
            clusterConfig.filterConsumers,
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
              clusterOperation.clusterConfig.pollConsumers,
              clusterOperation.clusterConfig.filterConsumers,
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
