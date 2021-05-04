/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import features.{ApplicationFeatures, KMReassignPartitionsFeature}
import kafka.manager.ApiError
import kafka.manager.model.ActorModel._
import models.FollowLink
import models.form.ReassignPartitionOperation.{ForceRunAssignment, RunAssignment, UnknownRPO}
import models.form._
import models.navigation.Menus
import play.api.data.Form
import play.api.data.Forms._
import play.api.data.validation.{Constraint, Invalid, Valid}
import play.api.i18n.I18nSupport
import play.api.mvc._
import scalaz.{-\/, \/, \/-}

import scala.concurrent.{ExecutionContext, Future}

/**
 * @author hiral
 */
class ReassignPartitions (val cc: ControllerComponents, val kafkaManagerContext: KafkaManagerContext)
                         (implicit af: ApplicationFeatures, menus: Menus, ec:ExecutionContext)  extends AbstractController(cc) with I18nSupport {

  private[this] implicit val kafkaManager = kafkaManagerContext.getKafkaManager

  val validateOperation : Constraint[String] = Constraint("validate operation value") {
    case "confirm" => Valid
    case "force" => Valid
    case "run" => Valid
    case "generate" => Valid
    case any: Any => Invalid(s"Invalid operation value: $any")
  }


  val reassignPartitionsForm = Form(
    mapping(
      "operation" -> nonEmptyText.verifying(validateOperation)
    )(ReassignPartitionOperation.withNameInsensitiveOption)(op => op.map(_.entryName))
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
      },
      "replicationFactor" -> optional(number(min = 1))
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

  def reassignPartitions(c: String) = Action.async { implicit request:RequestHeader =>
    kafkaManager.getReassignPartitions(c).map { errorOrStatus =>
      Ok(views.html.reassignPartitions(c,errorOrStatus)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
    }
  }

  def runMultipleAssignments(c: String) = Action.async { implicit request:RequestHeader =>
    featureGate(KMReassignPartitionsFeature) {
      kafkaManager.getTopicList(c).flatMap { errorOrSuccess =>
        withClusterContext(c)(
          err => Future.successful(
            Ok(views.html.errors.onApiError(err, Option(FollowLink("Try Again", routes.ReassignPartitions.runMultipleAssignments(c).toString())))).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
          ),
          cc => Future.successful(
            Ok(views.html.topic.runMultipleAssignments(
              c, errorOrSuccess.map(l => 
                (reassignMultipleTopicsForm.fill(RunMultipleAssignments(l.list.map(TopicSelect.from))), cc))
            )).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
          )
        )
      }
    }
  }

  def confirmAssignment(c: String, t: String) = Action.async { implicit request:RequestHeader =>
    featureGate(KMReassignPartitionsFeature) {
      kafkaManager.getBrokerList(c).flatMap { errorOrSuccess =>
        withClusterContext(c)(
          err => Future.successful(
            Ok(views.html.errors.onApiError(err, Option(FollowLink("Try Again", routes.ReassignPartitions.confirmAssignment(c, t).toString())))).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
          ),
          cc =>
            kafkaManager.getGeneratedAssignments(c, t).map { errorOrAssignments =>
              Ok(views.html.topic.confirmAssignment(
                c, t, errorOrSuccess.map(l =>
                  (generateAssignmentsForm.fill(GenerateAssignment(l.list.map(BrokerSelect.from))), cc)
                ),
                errorOrAssignments
              )).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
            }
        )
      }
    }
  }

  def confirmMultipleAssignments(c: String) = Action.async { implicit request:RequestHeader =>
    featureGate(KMReassignPartitionsFeature) {
      kafkaManager.getTopicList(c).flatMap { errOrTL =>
        withClusterContext(c)(
          err => Future.successful(
            Ok(views.html.errors.onApiError(err, Option(FollowLink("Try Again", routes.ReassignPartitions.confirmMultipleAssignments(c).toString())))).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
          ),
          cc =>
            errOrTL.fold(
            { err: ApiError =>
              Future.successful(Ok(views.html.topic.confirmMultipleAssignments(c, -\/(err))).withHeaders("X-Frame-Options" -> "SAMEORIGIN"))
            }, { tL: TopicList =>
              kafkaManager.getBrokerList(c).map { errorOrSuccess =>
                Ok(views.html.topic.confirmMultipleAssignments(
                  c, errorOrSuccess.map(l => 
                    (generateMultipleAssignmentsForm.fill(GenerateMultipleAssignments(tL.list.map(TopicSelect.from), l.list.map(BrokerSelect.from))),
                     cc)
                  )
                )).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
              }
            }
            )
        )
      }
    }
  }
  
  private[this] def flattenTopicIdentity(td: TopicIdentity) = {
    (td.topic, td.partitionsIdentity.toList.map { case (partition, identity) =>
      (partition, identity.replicas.toList)
    })
  }

  def manualAssignments(c: String, t: String): Action[AnyContent] = Action.async { implicit request:RequestHeader =>
    featureGate(KMReassignPartitionsFeature) {
      
      withClusterFeatures(c)( err => {
        Future.successful(Ok(views.html.errors.onApiError(err,
          Option(FollowLink("Try Again", routes.ReassignPartitions.manualAssignments(c, t).toString())))).withHeaders("X-Frame-Options" -> "SAMEORIGIN"))
      }, implicit clusterFeatures => {
        val futureTopicIdentity = kafkaManager.getTopicIdentity(c, t)
        val futureBrokersViews = kafkaManager.getBrokersView(c)
        val futureBrokerList = kafkaManager.getBrokerList(c)

        /*
      def flattenedTopicListExtended(topicListExtended: TopicListExtended) = {
        topicListExtended.list
          .filter(_._2.isDefined)
          .sortBy(_._1)
          .slice(offset, offset+maxResults)
          .map(tpl => flattenTopicIdentity(tpl._2.get)).toList
      }*/

        val futureResult: Future[Result] = for {
          tiOrError <- futureTopicIdentity
          bvOrError <- futureBrokersViews
          blOrError <- futureBrokerList
        } yield {
          val errorOrResult: ApiError \/ Result = for {
            ti <- tiOrError
            bv <- bvOrError
            bl <- blOrError
          } yield {
            Ok(views.html.topic.manualAssignments(
              //c, t, manualReassignmentForm.fill(List(flattenTopicIdentity(ti))), bl, bv, manualReassignmentForm.errors
              c, t, List(flattenTopicIdentity(ti)), bl, bv, manualReassignmentForm.errors
            )).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
          }
          errorOrResult.fold(err => {
            Ok(views.html.errors.onApiError(err,
              Option(FollowLink("Try Again", routes.ReassignPartitions.manualAssignments(c, t).toString())))).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
          }, identity[Result])
        }

        futureResult.recover {
          case err =>
            Ok(views.html.errors.onApiError(ApiError(s"Unknown error : ${err.getMessage}"),
              Option(FollowLink("Try Again", routes.ReassignPartitions.manualAssignments(c, t).toString())))).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
        }

        /*
      topicList.flatMap { errOrTL =>
        errOrTL.fold(
        { err: ApiError =>
          Future.successful(Ok(views.html.topic.confirmMultipleAssignments(c, -\/(err))))
        }, { topics: TopicListExtended =>
          kafkaManager.getBrokerList(c).flatMap { errOrCV =>
            errOrCV.fold(
            {err: ApiError =>
              Future.successful( Ok(views.html.topic.confirmMultipleAssignments( c, -\/(err) )))
            },
            { brokers: BrokerListExtended => {
                brokersViews.flatMap { errorOrBVs =>
                  errorOrBVs.fold (
                  {err: ApiError => Future.successful( Ok(views.html.topic.confirmMultipleAssignments( c, -\/(err) )))},
                  {bVs => Future {
                    Ok(views.html.topic.manualMultipleAssignments(
                      c, flattenedTopicListExtended(topics), brokers , bVs, manualReassignmentForm.errors
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
      }*/
      }
      )
    }
  }

  def handleManualAssignment(c: String, t: String) = Action.async { implicit request:Request[AnyContent] =>
    featureGate(KMReassignPartitionsFeature) {
      def validateAssignment(assignment: List[(String, List[(Int, List[Int])])]) = {
        (for {
          (topic, assign) <- assignment
          (partition, replicas) <- assign
        } yield {
          replicas.size == replicas.toSet.size
        }) forall { b => b}
      }

      def responseScreen(title: String, errorOrResult: \/[IndexedSeq[ApiError], Unit]): Future[Result] = {
        withClusterFeatures(c)( err => {
          Future.successful(Ok(views.html.errors.onApiError(err,
            Option(FollowLink("Try Again", routes.ReassignPartitions.manualAssignments(c, t).toString())))).withHeaders("X-Frame-Options" -> "SAMEORIGIN"))
        }, implicit clusterFeatures => {
          Future.successful(Ok(views.html.common.resultsOfCommand(
            views.html.navigation.clusterMenu(c, title, "", menus.clusterMenus(c)),
            models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Manual Reassignment View", c, "", title),
            errorOrResult,
            title,
            FollowLink("Go to topic view.", routes.Topic.topic(c, t).toString()),
            FollowLink("Try again.", routes.Topic.topics(c).toString())
          )).withHeaders("X-Frame-Options" -> "SAMEORIGIN"))
        })
      }

      manualReassignmentForm.bindFromRequest.fold(
        errors => kafkaManager.getClusterList.flatMap { errorOrClusterList =>
          responseScreen(
            "Manual Reassign Partitions Failure",
            -\/(IndexedSeq(ApiError("There is something really wrong with your submitted data!\n\n" + errors.toString)))
          )
        },
        assignment => {
          if (validateAssignment(assignment)) {
            kafkaManager.manualPartitionAssignments(c, assignment).flatMap { errorOrClusterList =>
              responseScreen("Manual Partitions Reassignment Successful", errorOrClusterList)
            }
          } else {
            responseScreen(
              "Manual Partitions Reassignment Failure",
              -\/(IndexedSeq(ApiError("You cannot (or at least should not) assign two replicas of the same partition to the same broker!!")))
            )
          }
        }
      )
    }
  }

  def handleGenerateAssignment(c: String, t: String) = Action.async { implicit request:Request[AnyContent] =>
    featureGate(KMReassignPartitionsFeature) {
      withClusterContext(c)(
        err => Future.successful(
          Ok(views.html.errors.onApiError(err, Option(FollowLink("Try Again", routes.Topic.topic(c, t).toString())))).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
        ),
        cc =>
          generateAssignmentsForm.bindFromRequest.fold(
            errors => {
              kafkaManager.getGeneratedAssignments(c, t).map { errorOrAssignments =>
                Ok(views.html.topic.confirmAssignment(c, t, \/-((errors, cc)), errorOrAssignments)).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
              }
            },
            assignment => {
              kafkaManager.generatePartitionAssignments(c, Set(t), assignment.brokers.filter(_.selected).map(_.id).toSet, assignment.replicationFactor).map { errorOrSuccess =>
                implicit val clusterFeatures = cc.clusterFeatures
                Ok(views.html.common.resultsOfCommand(
                  views.html.navigation.clusterMenu(c, "Reassign Partitions", "", menus.clusterMenus(c)),
                  models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View", c, t, "Generate Partition Assignments"),
                  errorOrSuccess,
                  s"Generate Partition Assignments - $t",
                  FollowLink("Go to topic view.", routes.Topic.topic(c, t).toString()),
                  FollowLink("Try again.", routes.Topic.topic(c, t).toString())
                )).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
              }
            }
          )
      )
    }
  }

  def handleGenerateMultipleAssignments(c: String) = Action.async { implicit request:Request[AnyContent] =>
    featureGate(KMReassignPartitionsFeature) {
      withClusterContext(c)(
        err => Future.successful(
          Ok(views.html.errors.onApiError(err, Option(FollowLink("Try Again", routes.Topic.topics(c).toString())))).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
        ),
        cc =>
          generateMultipleAssignmentsForm.bindFromRequest.fold(
            errors => Future.successful(Ok(views.html.topic.confirmMultipleAssignments(c, \/-((errors, cc)))).withHeaders("X-Frame-Options" -> "SAMEORIGIN")),
            assignment => {
              kafkaManager.generatePartitionAssignments(c, assignment.topics.filter(_.selected).map(_.name).toSet, assignment.brokers.filter(_.selected).map(_.id).toSet).map { errorOrSuccess =>
                implicit val clusterFeatures = cc.clusterFeatures
                Ok(views.html.common.resultsOfCommand(
                  views.html.navigation.clusterMenu(c, "Reassign Partitions", "", menus.clusterMenus(c)),
                  models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View", c, "", "Generate Partition Assignments"),
                  errorOrSuccess,
                  s"Generate Partition Assignments",
                  FollowLink("Go to topic list.", routes.Topic.topics(c).toString()),
                  FollowLink("Try again.", routes.Topic.topics(c).toString())
                )).withHeaders("X-Frame-Options" -> "SAMEORIGIN")

              }
            }
          )
      )
    }
  }
  
  def handleRunMultipleAssignments(c: String) = Action.async { implicit request:Request[AnyContent] =>
    featureGate(KMReassignPartitionsFeature) {
      withClusterContext(c)(
        err => Future.successful(
          Ok(views.html.errors.onApiError(err, Option(FollowLink("Try Again", routes.Topic.topics(c).toString())))).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
        ),
        cc =>
          reassignMultipleTopicsForm.bindFromRequest.fold(
            errors => Future.successful(Ok(views.html.topic.runMultipleAssignments(c, \/-((errors, cc)))).withHeaders("X-Frame-Options" -> "SAMEORIGIN")),
            assignment => {
              kafkaManager
                .runReassignPartitions(c, assignment.topics.filter(_.selected).map(_.name).toSet)
                .map { errorOrSuccess =>
                implicit val clusterFeatures = cc.clusterFeatures
                Ok(
                  views.html.common.resultsOfCommand(
                    views.html.navigation.clusterMenu(c, "Reassign Partitions", "", menus.clusterMenus(c)),
                    models.navigation.BreadCrumbs.withNamedViewAndCluster("Topics", c, "Reassign Partitions"),
                    errorOrSuccess,
                    s"Run Reassign Partitions",
                    FollowLink("Go to reassign partitions.", routes.ReassignPartitions.reassignPartitions(c).toString()),
                    FollowLink("Try again.", routes.Topic.topics(c).toString())
                  )
                ).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
              }
            }
          )
      )
    }
  }

  def handleOperation(c: String, t: String) = Action.async { implicit request:Request[AnyContent] =>
    featureGate(KMReassignPartitionsFeature) {
      withClusterContext(c)(
        err => Future.successful(
          Ok(views.html.errors.onApiError(err, Option(FollowLink("Try Force Running", routes.Topic.topic(c, t, force = err.recoverByForceOperation).toString())))).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
        ),
        cc =>
          reassignPartitionsForm.bindFromRequest.fold(
            formWithErrors => Future.successful(BadRequest(views.html.topic.topicView(c, t, -\/(ApiError("Unknown operation!")), None, UnknownRPO))),
            op => op match {
              case Some(RunAssignment) =>
                implicit val clusterFeatures = cc.clusterFeatures
                kafkaManager.runReassignPartitions(c, Set(t)).map { errorOrSuccess =>
                  Ok(views.html.common.resultsOfCommand(
                    views.html.navigation.clusterMenu(c, "Reassign Partitions", "", menus.clusterMenus(c)),
                    models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View", c, t, "Run Reassign Partitions"),
                    errorOrSuccess,
                    s"Run Reassign Partitions - $t",
                    FollowLink("Go to reassign partitions.", routes.ReassignPartitions.reassignPartitions(c).toString()),
                    FollowLink("Try force running!", routes.Topic.topic(c, t, force = true).toString())
                  )).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
                }
              case Some(ForceRunAssignment) =>
                implicit val clusterFeatures = cc.clusterFeatures
                kafkaManager.runReassignPartitions(c, Set(t), force = true).map { errorOrSuccess =>
                  Ok(views.html.common.resultsOfCommand(
                    views.html.navigation.clusterMenu(c, "Reassign Partitions", "", menus.clusterMenus(c)),
                    models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View", c, t, "Run Reassign Partitions"),
                    errorOrSuccess,
                    s"Run Reassign Partitions - $t",
                    FollowLink("Go to reassign partitions.", routes.ReassignPartitions.reassignPartitions(c).toString()),
                    FollowLink("Try again.", routes.Topic.topic(c, t).toString())
                  )).withHeaders("X-Frame-Options" -> "SAMEORIGIN")
                }
              case unknown =>
                implicit val clusterFeatures = cc.clusterFeatures
                Future.successful(Ok(views.html.common.resultOfCommand(
                  views.html.navigation.clusterMenu(c, "Reassign Partitions", "", menus.clusterMenus(c)),
                  models.navigation.BreadCrumbs.withNamedViewAndClusterAndTopic("Topic View", c, t, "Unknown Reassign Partitions Operation"),
                  -\/(ApiError(s"Unknown operation $unknown")),
                  "Unknown Reassign Partitions Operation",
                  FollowLink("Back to reassign partitions.", routes.ReassignPartitions.reassignPartitions(c).toString()),
                  FollowLink("Back to reassign partitions.", routes.ReassignPartitions.reassignPartitions(c).toString())
                )).withHeaders("X-Frame-Options" -> "SAMEORIGIN"))
            }
          )
      )
    }
  }
}
