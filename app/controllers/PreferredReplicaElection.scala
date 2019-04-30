/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import features.{ApplicationFeatures, KMPreferredReplicaElectionFeature}
import kafka.manager.ApiError
import kafka.manager.features.ClusterFeatures
import models.FollowLink
import models.form.{PreferredReplicaElectionOperation, RunElection, UnknownPREO}
import models.navigation.Menus
import play.api.data.Form
import play.api.data.Forms._
import play.api.data.validation.{Constraint, Invalid, Valid}
import play.api.i18n.I18nSupport
import play.api.mvc._

import scala.concurrent.{ExecutionContext, Future}
import scalaz.-\/

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
}
