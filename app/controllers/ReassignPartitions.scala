/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import kafka.manager.ApiError
import models.navigation.Menus
import models.{navigation, FollowLink}
import models.form._
import play.api.data.Form
import play.api.data.Forms._
import play.api.data.validation.{Valid, Invalid, Constraint}
import play.api.mvc._

import scala.concurrent.Future
import scalaz.-\/

/**
 * @author hiral
 */
object ReassignPartitions extends Controller{
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = KafkaManagerContext.getKafkaManger

  val validateOperation : Constraint[String] = Constraint("validate operation value") {
    case "run" => Valid
    case "generate" => Valid
    case any: Any => Invalid(s"Invalid operation value: $any")
  }

  val reassignPartitionsForm = Form(
    mapping(
      "operation" -> nonEmptyText.verifying(validateOperation)
    )(ReassignPartitionOperation.apply)(ReassignPartitionOperation.unapply)
  )

  def reassignPartitions(c: String) = Action.async {
    kafkaManager.getReassignPartitions(c).map { errorOrStatus =>
      Ok(views.html.reassignPartitions(c,errorOrStatus))
    }
  }

  def handleOperation(c: String, t: String) = Action.async { implicit request =>
    reassignPartitionsForm.bindFromRequest.fold(
      formWithErrors => Future.successful(BadRequest(views.html.topic.topicView(c,t,-\/(ApiError("Unknown operation!"))))),
      op => op match {
        case GenerateAssignment =>
          kafkaManager.generatePartitionAssignments(c,Set(t)).map { errorOrSuccess =>
            Ok(views.html.common.resultsOfCommand(
              views.html.navigation.clusterMenu(c,"Reassign Partitions","",Menus.clusterMenus(c)),
              models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View",c,t,"Generate Partition Assignments"),
              errorOrSuccess,
              s"Generate Partition Assignments - $t",
              FollowLink("Go to topic view.",routes.Application.topic(c,t).toString()),
              FollowLink("Try again.",routes.Application.topic(c,t).toString())
            ))
          }
        case RunAssignment =>
          kafkaManager.runReassignPartitions(c,Set(t)).map { errorOrSuccess =>
            Ok(views.html.common.resultsOfCommand(
              views.html.navigation.clusterMenu(c,"Reassign Partitions","",navigation.Menus.clusterMenus(c)),
              models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View",c,t,"Run Reassign Partitions"),
              errorOrSuccess,
              s"Run Reassign Partitions - $t",
              FollowLink("Go to reassign partitions.",routes.ReassignPartitions.reassignPartitions(c).toString()),
              FollowLink("Try again.",routes.Application.topic(c,t).toString())
            ))
          }
        case UnknownRPO(opString) =>
          Future.successful(Ok(views.html.common.resultOfCommand(
            views.html.navigation.clusterMenu(c,"Reassign Partitions","",navigation.Menus.clusterMenus(c)),
            models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View",c,t,"Unknown Reassign Partitions Operation"),
            -\/(ApiError(s"Unknown operation $opString")),
            "Unknown Reassign Partitions Operation",
            FollowLink("Back to reassign partitions.",routes.ReassignPartitions.reassignPartitions(c).toString()),
            FollowLink("Back to reassign partitions.",routes.ReassignPartitions.reassignPartitions(c).toString())
          )))
      }
    )
  }
}
