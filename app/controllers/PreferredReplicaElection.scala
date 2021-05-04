/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import features.{ApplicationFeatures, KMPreferredReplicaElectionFeature, KMScheduleLeaderElectionFeature}
import kafka.manager.ApiError
import kafka.manager.features.ClusterFeatures
import models.FollowLink
import models.form.{PreferredReplicaElectionOperation, RunElection, UnknownPREO}
import models.navigation.Menus
import play.api.data.Form
import play.api.data.Forms._
import play.api.data.validation.{Constraint, Invalid, Valid}
import play.api.i18n.I18nSupport
import play.api.libs.json.{JsObject, Json}
import play.api.mvc._
import scalaz.-\/

import scala.concurrent.{ExecutionContext, Future}

/**
 * @author hiral
 */
class PreferredReplicaElection (val cc: ControllerComponents, val kafkaManagerContext: KafkaManagerContext)
                               (implicit af: ApplicationFeatures, menus: Menus, ec:ExecutionContext) extends AbstractController(cc) with I18nSupport {

  private[this] val kafkaManager = kafkaManagerContext.getKafkaManager
  private[this] implicit val cf: ClusterFeatures = ClusterFeatures.default


  val validateOperation : Constraint[String] = Constraint("validate operation value") {
    case "run" => Valid
    case any: Any => Invalid(s"Invalid operation value: $any")
  }

  val preferredReplicaElectionForm = Form(
    mapping(
      "operation" -> nonEmptyText.verifying(validateOperation)
    )(PreferredReplicaElectionOperation.apply)(PreferredReplicaElectionOperation.unapply)
  )

  def preferredReplicaElection(c: String) = Action.async { implicit request: RequestHeader =>
    kafkaManager.getPreferredLeaderElection(c).map { errorOrStatus =>
      Ok(views.html.preferredReplicaElection(c,errorOrStatus,preferredReplicaElectionForm)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
    }
  }


  def handleRunElection(c: String) = Action.async { implicit request: Request[AnyContent] =>
    featureGate(KMPreferredReplicaElectionFeature) {
      preferredReplicaElectionForm.bindFromRequest.fold(
        formWithErrors => Future.successful(BadRequest(views.html.preferredReplicaElection(c, -\/(ApiError("Unknown operation!")), formWithErrors))),
        op => op match {
          case RunElection =>
            val errorOrSuccessFuture = kafkaManager.getTopicList(c).flatMap { errorOrTopicList =>
              errorOrTopicList.fold({ e =>
                Future.successful(-\/(e))
              }, { topicList =>
                kafkaManager.runPreferredLeaderElection(c, topicList.list.toSet)
              })
            }
            errorOrSuccessFuture.map { errorOrSuccess =>
              Ok(views.html.common.resultOfCommand(
                views.html.navigation.clusterMenu(c, "Preferred Replica Election", "", menus.clusterMenus(c)),
                models.navigation.BreadCrumbs.withViewAndCluster("Run Election", c),
                errorOrSuccess,
                "Run Election",
                FollowLink("Go to preferred replica election.", routes.PreferredReplicaElection.preferredReplicaElection(c).toString()),
                FollowLink("Try again.", routes.PreferredReplicaElection.preferredReplicaElection(c).toString())
              )).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
            }
          case UnknownPREO(opString) =>
            Future.successful(Ok(views.html.common.resultOfCommand(
              views.html.navigation.clusterMenu(c, "Preferred Replica Election", "", menus.clusterMenus(c)),
              models.navigation.BreadCrumbs.withNamedViewAndCluster("Preferred Replica Election", c, "Unknown Operation"),
              -\/(ApiError(s"Unknown operation $opString")),
              "Unknown Preferred Replica Election Operation",
              FollowLink("Back to preferred replica election.", routes.PreferredReplicaElection.preferredReplicaElection(c).toString()),
              FollowLink("Back to preferred replica election.", routes.PreferredReplicaElection.preferredReplicaElection(c).toString())
            )).withHeaders("X-Frame-Options" -> "SAMEORIGIN"))
        }
      )
    }
  }

  def handleScheduledIntervalAPI(cluster: String): Action[AnyContent] = Action.async { implicit request =>
    featureGate(KMScheduleLeaderElectionFeature) {
      val interval = kafkaManager.pleCancellable.get(cluster).map(_._2).getOrElse(0)
      Future(Ok(Json.obj("scheduledInterval" -> interval))
        .withHeaders("X-Frame-Options" -> "SAMEORIGIN"))
    }
  }

  def scheduleRunElection(c: String) = Action.async { implicit request =>
    def getOrZero : (Int, String) = if(kafkaManager.pleCancellable.contains(c)){
      (kafkaManager.pleCancellable(c)._2, "Scheduler is running")
    }
    else {
      (0, "Scheduler is not running")
    }
    val (timePeriod, status_string) = getOrZero
    kafkaManager.getTopicList(c).map { errorOrStatus =>
      Ok(views.html.scheduleLeaderElection(c,errorOrStatus, status_string, timePeriod)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
    }
  }

  def handleScheduleRunElection(c: String) = Action.async { implicit request =>
    def setOrExtract : (Int, String) = if(!kafkaManager.pleCancellable.contains(c)){
      kafkaManager.getTopicList(c).flatMap { errorOrTopicList =>
        errorOrTopicList.fold({ e =>
          Future.successful(-\/(e))
        }, { topicList =>
          kafkaManager.schedulePreferredLeaderElection(c, topicList.list.toSet, request.body.asFormUrlEncoded.get("timePeriod")(0).toInt)
        })
      }
      (request.body.asFormUrlEncoded.get("timePeriod")(0).toInt, "Scheduler started")
    }
    else{
      (kafkaManager.pleCancellable(c)._2, "Scheduler already scheduled")
    }
    val (timeIntervalMinutes, status_string) = setOrExtract
    kafkaManager.getTopicList(c).map { errorOrStatus =>
      Ok(views.html.scheduleLeaderElection(c, errorOrStatus, status_string, timeIntervalMinutes)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
    }
  }

  def cancelScheduleRunElection(c: String) = Action.async { implicit request =>
    val status_string: String = if(kafkaManager.pleCancellable.contains(c)){
      kafkaManager.cancelPreferredLeaderElection(c)
      "Scheduler stopped"
    }
    else "Scheduler already not running"
    kafkaManager.getTopicList(c).map { errorOrStatus =>
      Ok(views.html.scheduleLeaderElection(c,errorOrStatus,status_string, 0)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
    }
  }

  def handleScheduleRunElectionAPI(c: String) = Action.async { implicit request =>
    // ToDo: Refactor out common part from handleScheduleRunElection
    featureGate(KMScheduleLeaderElectionFeature) {
      def setOrExtract : (Int, String) = if(!kafkaManager.pleCancellable.contains(c)){
        kafkaManager.getTopicList(c).flatMap { errorOrTopicList =>
          errorOrTopicList.fold({ e =>
            Future.successful(-\/(e))
          }, { topicList =>
            kafkaManager.schedulePreferredLeaderElection(c, topicList.list.toSet, request.body.asInstanceOf[AnyContentAsJson].json.asInstanceOf[JsObject].values.toList(0).toString().toInt)
          })
        }
        (request.body.asInstanceOf[AnyContentAsJson].json.asInstanceOf[JsObject].values.toList(0).toString().toInt, "Scheduler started")
      }
      else{
        (kafkaManager.pleCancellable(c)._2, "Scheduler already scheduled")
      }
      val (timePeriod, status_string) = setOrExtract
      Future(
        Ok(Json.obj(
          "scheduledInterval" -> timePeriod, "message" -> status_string
        )).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
      )
    }
  }

  def cancelScheduleRunElectionAPI(c: String) = Action.async { implicit request =>
    // ToDo: Refactor out common part from cancelScheduleRunElection
    featureGate(KMScheduleLeaderElectionFeature) {
      val status_string: String = if(kafkaManager.pleCancellable.contains(c)){
        kafkaManager.cancelPreferredLeaderElection(c)
        "Scheduler stopped"
      }
      else "Scheduler already not running"
      Future(Ok(Json.obj("scheduledInterval" -> 0, "message" -> status_string)).withHeaders("X-Frame-Options" -> "SAMEORIGIN"))
    }
  }
}
