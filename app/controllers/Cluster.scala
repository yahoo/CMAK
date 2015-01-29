/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import kafka.manager.{KafkaVersion, ApiError, ClusterConfig}
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

  private[this] val kafkaManager = KafkaManagerContext.getKafkaManger

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
      "zkMaxRetry" -> ignored(100 : Int)
    )(ClusterConfig.apply)(ClusterConfig.customUnapply)
  )

  val updateForm = Form(
    mapping(
      "operation" -> nonEmptyText.verifying(validateOperation),
      "name" -> nonEmptyText.verifying(maxLength(250), validateName),
      "kafkaVersion" -> nonEmptyText.verifying(validateKafkaVersion),
      "zkHosts" -> nonEmptyText.verifying(validateZkHosts),
      "zkMaxRetry" -> ignored(100 : Int)
    )(ClusterOperation.apply)(ClusterOperation.customUnapply)
  )

  def addCluster = Action.async { implicit request =>
    Future.successful(Ok(views.html.cluster.addCluster(clusterConfigForm)))
  }

  def updateCluster(c: String) = Action.async { implicit request =>
    kafkaManager.getClusterConfig(c).map { errorOrClusterConfig =>
      Ok(views.html.cluster.updateCluster(c,errorOrClusterConfig.map { cc =>
        updateForm.fill(ClusterOperation.apply(Update.toString,cc.name,cc.version.toString,cc.curatorConfig.zkConnect,cc.curatorConfig.zkMaxRetry))
      }))
    }
  }

  def handleAddCluster = Action.async { implicit request =>
    clusterConfigForm.bindFromRequest.fold(
      formWithErrors => Future.successful(BadRequest(views.html.cluster.addCluster(formWithErrors))),
      clusterConfig => {
        kafkaManager.addCluster(clusterConfig.name, clusterConfig.version.toString, clusterConfig.curatorConfig.zkConnect).map { errorOrSuccess =>
          Ok(views.html.common.resultOfCommand(
            views.html.navigation.defaultMenu(),
            models.navigation.BreadCrumbs.withView("Add Cluster"),
            errorOrSuccess,
            "Add Cluster",
            FollowLink("Go to cluster view.",routes.Application.cluster(clusterConfig.name).toString()),
            FollowLink("Try again.",routes.Cluster.addCluster().toString())
          ))
        }
      }
    )
  }

  def handleUpdateCluster(c: String) = Action.async { implicit request =>
    updateForm.bindFromRequest.fold(
      formWithErrors => Future.successful(BadRequest(views.html.cluster.updateCluster(c,\/-(formWithErrors)))),
      clusterOperation => clusterOperation.op match {
        case Enable =>
          kafkaManager.enableCluster(c).map { errorOrSuccess =>
            Ok(views.html.common.resultOfCommand(
              views.html.navigation.defaultMenu(),
              models.navigation.BreadCrumbs.withViewAndCluster("Enable Cluster",c),
              errorOrSuccess,
              "Enable Cluster",
              FollowLink("Go to cluster list.",routes.Application.index().toString()),
              FollowLink("Back to cluster list.",routes.Application.index().toString())
            ))
          }
        case Disable =>
          kafkaManager.disableCluster(c).map { errorOrSuccess =>
            Ok(views.html.common.resultOfCommand(
              views.html.navigation.defaultMenu(),
              models.navigation.BreadCrumbs.withViewAndCluster("Disable Cluster",c),
              errorOrSuccess,
              "Disable Cluster",
              FollowLink("Back to cluster list.",routes.Application.index().toString()),
              FollowLink("Back to cluster list.",routes.Application.index().toString())
            ))
          }
        case Delete =>
          kafkaManager.deleteCluster(c).map { errorOrSuccess =>
            Ok(views.html.common.resultOfCommand(
              views.html.navigation.defaultMenu(),
              models.navigation.BreadCrumbs.withViewAndCluster("Delete Cluster",c),
              errorOrSuccess,
              "Delete Cluster",
              FollowLink("Back to cluster list.",routes.Application.index().toString()),
              FollowLink("Back to cluster list.",routes.Application.index().toString())
            ))
          }
        case Update =>
          kafkaManager.updateCluster(
            clusterOperation.clusterConfig.name,
            clusterOperation.clusterConfig.version.toString,
            clusterOperation.clusterConfig.curatorConfig.zkConnect
          ).map { errorOrSuccess =>
            Ok(views.html.common.resultOfCommand(
              views.html.navigation.defaultMenu(),
              models.navigation.BreadCrumbs.withViewAndCluster("Update Cluster",c),
              errorOrSuccess,
              "Update Cluster",
              FollowLink("Go to cluster view.",routes.Application.cluster(clusterOperation.clusterConfig.name).toString()),
              FollowLink("Try again.",routes.Cluster.updateCluster(c).toString())
            ))
          }
        case Unknown(opString) =>
          Future.successful(Ok(views.html.common.resultOfCommand(
            views.html.navigation.defaultMenu(),
            models.navigation.BreadCrumbs.withViewAndCluster("Unknown Cluster Operation",c),
            -\/(ApiError(s"Unknown operation $opString")),
            "Unknown Cluster Operation",
            FollowLink("Back to cluster list.",routes.Application.index().toString()),
            FollowLink("Back to cluster list.",routes.Application.index().toString())
          )))
      }
    )
  }
}
