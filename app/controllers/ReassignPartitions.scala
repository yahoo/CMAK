/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import kafka.manager.ActorModel._
import kafka.manager.{BrokerListExtended, ApiError, TopicListExtended}
import models.navigation.Menus
import models.{navigation, FollowLink}
import models.form._
import play.api.data.Form
import play.api.data.Forms._
import play.api.data.validation.{Valid, Invalid, Constraint}
import play.api.mvc._

import scala.concurrent.Future
import scalaz.{\/, \/-, -\/}

/**
 * @author hiral
 */
object ReassignPartitions extends Controller{
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = KafkaManagerContext.getKafkaManager

  val validateOperation : Constraint[String] = Constraint("validate operation value") {
    case "confirm" => Valid
    case "run" => Valid
    case "generate" => Valid
    case any: Any => Invalid(s"Invalid operation value: $any")
  }


  val reassignPartitionsForm = Form(
    mapping(
      "operation" -> nonEmptyText.verifying(validateOperation)
    )(ReassignPartitionOperation.apply)(ReassignPartitionOperation.unapply)
  )
  
  val reassignMultipleTopicsForm = Form(
    mapping(
      "topics" -> seq {
        mapping(
          "name" -> nonEmptyText,
          "selected" -> boolean
        )(TopicSelect.apply)(TopicSelect.unapply)
      }
    )(RunMultipleAssignments.apply)(RunMultipleAssignments.unapply)
  )

  val manualReassignmentForm: Form[List[(String, List[(Int, List[Int])])]] = Form(
    "topics" -> list (
      tuple (
        "topic" -> text,
        "assignments" -> list (
          tuple (
            "partition" -> number,
            "brokers" -> list(number)
          )
        )
      )
    )
  )

  val generateAssignmentsForm = Form(
    mapping(
      "brokers" -> seq {
        mapping(
          "id" -> number(min = 0),
          "host" -> nonEmptyText,
          "selected" -> boolean
        )(BrokerSelect.apply)(BrokerSelect.unapply)
      }
    )(GenerateAssignment.apply)(GenerateAssignment.unapply)
  )

  val generateMultipleAssignmentsForm = Form(
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
      }
    )(GenerateMultipleAssignments.apply)(GenerateMultipleAssignments.unapply)
  )

  def reassignPartitions(c: String) = Action.async {
    kafkaManager.getReassignPartitions(c).map { errorOrStatus =>
      Ok(views.html.reassignPartitions(c,errorOrStatus))
    }
  }

  def runMultipleAssignments(c: String) = Action.async {
    kafkaManager.getTopicList(c).map { errorOrSuccess =>
      Ok(views.html.topic.runMultipleAssignments(
        c, errorOrSuccess.map(l => reassignMultipleTopicsForm.fill(RunMultipleAssignments(l.list.map(TopicSelect.from))))
      ))
    }
  }

  def confirmAssignment(c: String, t: String) = Action.async {
    kafkaManager.getBrokerList(c).map { errorOrSuccess =>
      Ok(views.html.topic.confirmAssignment(
        c, t, errorOrSuccess.map(l => generateAssignmentsForm.fill(GenerateAssignment(l.list.map(BrokerSelect.from))))
      ))
    }
  }

  def confirmMultipleAssignments(c: String) = Action.async {
    kafkaManager.getTopicList(c).flatMap { errOrTL =>
      errOrTL.fold(
      { err: ApiError =>
        Future.successful( Ok(views.html.topic.confirmMultipleAssignments( c, -\/(err) )))
      }, { tL: TopicList =>
        kafkaManager.getBrokerList(c).map { errorOrSuccess =>
          Ok(views.html.topic.confirmMultipleAssignments(
            c, errorOrSuccess.map(l => generateMultipleAssignmentsForm.fill(GenerateMultipleAssignments(tL.list.map(TopicSelect.from), l.list.map(BrokerSelect.from))))
          ))
        }
      }
      )
    }
  }

  def manualMultipleAssignments(c: String): Action[AnyContent] = Action.async {
    val topicList = kafkaManager.getTopicListExtended(c)
    val brokersViews = kafkaManager.getBrokersView(c)

    def flattenedTopicListExtended(topicListExtended: TopicListExtended) = {
      topicListExtended.list.map {
        case (topic, Some(topicIdentity)) =>
          (topic, topicIdentity.partitionsIdentity.toList.map { case (partition, identity) =>
            (partition, identity.replicas.toList)
          })
        case (topic, None) => (topic, List[(Int, List[Int])]())
      } toList
    }

    topicList.flatMap { errOrTL =>
      errOrTL.fold(
      { err: ApiError =>
        Future.successful( Ok(views.html.topic.confirmMultipleAssignments( c, -\/(err) )))
      },
      { topics: TopicListExtended =>
          kafkaManager.getBrokerList(c).flatMap { errOrCV =>
            errOrCV.fold(
            {err: ApiError =>
              Future.successful( Ok(views.html.topic.confirmMultipleAssignments( c, -\/(err) )))
            },
            { brokers: BrokerListExtended => {
                brokersViews.flatMap { errorOrBVs =>
                  errorOrBVs.fold (
                  {err: ApiError => Future.successful( Ok(views.html.topic.confirmMultipleAssignments( c, -\/(err) )))},
                  {bVs: Seq[BVView] => Future {
                    Ok(views.html.topic.manualMultipleAssignments(
                      c, manualReassignmentForm.fill(flattenedTopicListExtended(topics)), brokers , bVs, manualReassignmentForm.errors
                    ))
                  }}
                  )
                }
              }
            }
            )
          }
        }
      )
    }
  }

  def handleManualAssignment(c: String) = Action.async { implicit request =>
    def validateAssignment(assignment: List[(String, List[(Int, List[Int])])]) = {
      (for {
        (topic, assign) <- assignment
        (partition, replicas) <- assign
      } yield {
        replicas.size == replicas.toSet.size
      }) forall { b => b }
    }

    def responseScreen(title: String, errorOrResult: \/[IndexedSeq[ApiError], Unit]) = {
      Ok(views.html.common.resultsOfCommand(
        views.html.navigation.clusterMenu(c, title, "", Menus.clusterMenus(c)),
        models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Manual Reassignment View", c, "", title),
        errorOrResult,
        title,
        FollowLink("Go to topic list.", routes.Topic.topics(c).toString()),
        FollowLink("Try again.", routes.Topic.topics(c).toString())
      ))
    }

    manualReassignmentForm.bindFromRequest.fold (
      errors => kafkaManager.getClusterList.map { errorOrClusterList =>
        responseScreen(
          "Manual Reassign Partitions Failure",
          -\/(IndexedSeq(ApiError("There is something really wrong with your submitted data!")))
        )
      },
      assignment => {
        if (validateAssignment(assignment)) {
          kafkaManager.manualPartitionAssignments(c, assignment).map { errorOrClusterList =>
            responseScreen("Manual Partitions Reassignment Successful", errorOrClusterList)
          }
        } else {
          Future {
            responseScreen(
              "Manual Partitions Reassignment Failure",
              -\/(IndexedSeq(ApiError("You cannot (or at least should not) assign two replicas of the same partition to the same broker!!")))
            )
          }
        }
      }
    )
  }

  def handleGenerateAssignment(c: String, t: String) = Action.async { implicit request =>
    generateAssignmentsForm.bindFromRequest.fold(
      errors => Future.successful( Ok(views.html.topic.confirmAssignment( c, t, \/-(errors) ))),
      assignment => {
        kafkaManager.generatePartitionAssignments(c, Set(t), assignment.brokers.filter(_.selected).map(_.id)).map { errorOrSuccess =>
          Ok(views.html.common.resultsOfCommand(
            views.html.navigation.clusterMenu(c, "Reassign Partitions", "", Menus.clusterMenus(c)),
            models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View", c, t, "Generate Partition Assignments"),
            errorOrSuccess,
            s"Generate Partition Assignments - $t",
            FollowLink("Go to topic view.", routes.Topic.topic(c, t).toString()),
            FollowLink("Try again.", routes.Topic.topic(c, t).toString())
          ))

        }
      }
    )
  }

  def handleGenerateMultipleAssignments(c: String) = Action.async { implicit request =>
    generateMultipleAssignmentsForm.bindFromRequest.fold(
      errors => Future.successful( Ok(views.html.topic.confirmMultipleAssignments( c, \/-(errors) ))),
      assignment => {
        kafkaManager.generatePartitionAssignments(c, assignment.topics.filter(_.selected).map(_.name).toSet, assignment.brokers.filter(_.selected).map(_.id)).map { errorOrSuccess =>
          Ok(views.html.common.resultsOfCommand(
            views.html.navigation.clusterMenu(c, "Reassign Partitions", "", Menus.clusterMenus(c)),
            models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View", c, "", "Generate Partition Assignments"),
            errorOrSuccess,
            s"Generate Partition Assignments",
            FollowLink("Go to topic list.", routes.Topic.topics(c).toString()),
            FollowLink("Try again.", routes.Topic.topics(c).toString())
          ))

        }
      }
    )
  }
  
  def handleRunMultipleAssignments(c: String) = Action.async { implicit request =>
    reassignMultipleTopicsForm.bindFromRequest.fold(
      errors => Future.successful( Ok(views.html.topic.runMultipleAssignments( c, \/-(errors) ))),
      assignment => {
        kafkaManager
          .runReassignPartitions(c, assignment.topics.filter(_.selected).map(_.name).toSet)
          .map { errorOrSuccess =>
          Ok(
            views.html.common.resultsOfCommand(
              views.html.navigation.clusterMenu(c, "Reassign Partitions", "", navigation.Menus.clusterMenus(c)),
              models.navigation.BreadCrumbs.withNamedViewAndCluster("Topics", c, "Reassign Partitions"),
              errorOrSuccess,
              s"Run Reassign Partitions",
              FollowLink("Go to reassign partitions.", routes.ReassignPartitions.reassignPartitions(c).toString()),
              FollowLink("Try again.", routes.Topic.topics(c).toString())
            )
          )
        }
      }
    )
  }

  def handleOperation(c: String, t: String) = Action.async { implicit request =>
    reassignPartitionsForm.bindFromRequest.fold(
      formWithErrors => Future.successful(BadRequest(views.html.topic.topicView(c,t,-\/(ApiError("Unknown operation!")),None))),
      op => op match {
        case RunAssignment =>
          kafkaManager.runReassignPartitions(c,Set(t)).map { errorOrSuccess =>
            Ok(views.html.common.resultsOfCommand(
              views.html.navigation.clusterMenu(c,"Reassign Partitions","",navigation.Menus.clusterMenus(c)),
              models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View",c,t,"Run Reassign Partitions"),
              errorOrSuccess,
              s"Run Reassign Partitions - $t",
              FollowLink("Go to reassign partitions.",routes.ReassignPartitions.reassignPartitions(c).toString()),
              FollowLink("Try again.",routes.Topic.topic(c,t).toString())
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
