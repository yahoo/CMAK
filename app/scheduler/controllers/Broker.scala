package scheduler.controllers

import controllers.KafkaManagerContext
import kafka.manager.ActorModel.SchedulerBrokerIdentity
import models.FollowLink
import scheduler.models.form._
import models.navigation.Menus
import play.api.data.Form
import play.api.data.Forms._
import play.api.data.validation.{Constraint, Constraints}
import play.api.mvc.{Action, Controller}
import play.api.data.format.Formats._

import scala.concurrent.Future
import scalaz.{-\/, \/-}

object Broker extends Controller{
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = KafkaManagerContext.getKafkaManager

  val validateCpus: Constraint[Double] = Constraints.min(minValue = 0.0)

  val defaultAddForm = Form(
    mapping(
      "id" -> number(min = 0),
      "cpus" -> optional(of[Double].verifying(validateCpus)),
      "mem" -> optional(longNumber),
      "heap" -> optional(longNumber),
      "port" -> optional(text),
      "bindAddress" -> optional(text),
      "constraints" -> optional(text),
      "options" -> optional(text),
      "log4jOptions" -> optional(text),
      "jvmOptions" -> optional(text),
      "stickinessPeriod" -> optional(text),
      "failover" -> mapping(
        "failoverDelay" -> optional(text),
        "failoverMaxDelay" -> optional(text),
        "failoverMaxTries" -> optional(number(0))
      )(Failover.apply)(Failover.unapply)
    )(AddBroker.apply)(AddBroker.unapply)
  )

  private val defaultF = AddBroker(1, None, None, None, None, None, None, None, None, None, None, Failover(None, None, None))

  private val addBrokerForm = defaultAddForm.fill(defaultF)


  val defaultUpdateForm = Form(
    mapping(
      "id" -> number(min = 0),
      "cpus" -> optional(of[Double].verifying(validateCpus)),
      "mem" -> optional(longNumber),
      "heap" -> optional(longNumber),
      "port" -> optional(text),
      "bindAddress" -> optional(text),
      "constraints" -> optional(text),
      "options" -> optional(text),
      "log4jOptions" -> optional(text),
      "jvmOptions" -> optional(text),
      "stickinessPeriod" -> optional(text),
      "failover" -> mapping(
        "failoverDelay" -> optional(text),
        "failoverMaxDelay" -> optional(text),
        "failoverMaxTries" -> optional(number(0))
      )(Failover.apply)(Failover.unapply)
    )(UpdateBroker.apply)(UpdateBroker.unapply)
  )

  private def defaultUpdateF(brokerId: Int) = UpdateBroker(brokerId, None, None, None, None, None, None, None, None, None, None, Failover(None, None, None))

  private def stringPairs(pairs: Seq[(String, String)]) =
    if (pairs.isEmpty) None
    else
      Some(pairs.map {
        case (key, v) => s"$key=$v"
      }.mkString(","))

  private def updateBrokerForm(schedulerName: String, bi: SchedulerBrokerIdentity) = {
    defaultUpdateForm.fill(UpdateBroker(bi.id, Option(bi.cpus), Option(bi.mem), Option(bi.heap), bi.port, bi.bindAddress, stringPairs(bi.constraints),
      stringPairs(bi.options), stringPairs(bi.log4jOptions), bi.jvmOptions, Option(bi.stickiness.period),
      Failover(Option(bi.failover.delay), Option(bi.failover.maxDelay), bi.failover.maxTries)))
  }

  def addBroker(schedulerName: String) = Action.async { implicit request =>
      Future.successful(Ok(scheduler.views.html.broker.addBroker(schedulerName, \/-(addBrokerForm))))
  }

  def handleAddBroker(schedulerName: String) = Action.async { implicit request =>
    defaultAddForm.bindFromRequest.fold(
      formWithErrors => Future.successful(BadRequest(scheduler.views.html.broker.addBroker(schedulerName,\/-(formWithErrors)))),
      ab => {
        kafkaManager.addBroker(schedulerName,ab.id, ab.cpus, ab.mem, ab.heap, ab.port, ab.bindAddress, ab.constraints, ab.options,
          ab.log4jOptions, ab.jvmOptions, ab.stickinessPeriod, ab.failover).map { errorOrSuccess =>
          Ok(views.html.common.resultOfCommand(
            views.html.navigation.schedulerMenu(schedulerName,"Broker","Add Broker",Menus.schedulerMenus(schedulerName)),
            models.navigation.BreadCrumbs.withNamedViewAndScheduler("Brokers",schedulerName,"Add Broker"),
            errorOrSuccess,
            "Add Broker",
            FollowLink("Go to broker view.",scheduler.controllers.routes.SchedulerApplication.broker(schedulerName, ab.id).toString()),
            FollowLink("Try again.",scheduler.controllers.routes.Broker.addBroker(schedulerName).toString())
          ))
        }
      }
    )
  }

  def handleStartBroker(schedulerName: String, id: Int) = Action.async { implicit request =>
    kafkaManager.startBroker(schedulerName, id).map { errorOrSuccess =>
      Ok(views.html.common.resultOfCommand(
        views.html.navigation.schedulerMenu(schedulerName, "Broker", "Broker View", Menus.schedulerMenus(schedulerName)),
        models.navigation.BreadCrumbs.withNamedViewAndScheduler("Brokers",schedulerName,"Start Broker"),
        errorOrSuccess,
        "Start Broker",
        FollowLink("Go to broker view.", scheduler.controllers.routes.SchedulerApplication.broker(schedulerName, id).toString()),
        FollowLink("Try again.", scheduler.controllers.routes.Broker.handleStartBroker(schedulerName, id).toString())
      ))
    }
  }

  def handleStopBroker(schedulerName: String, id: Int) = Action.async { implicit request =>
    kafkaManager.stopBroker(schedulerName, id).map { errorOrSuccess =>
      Ok(views.html.common.resultOfCommand(
        views.html.navigation.schedulerMenu(schedulerName, "Broker", "Broker View", Menus.schedulerMenus(schedulerName)),
        models.navigation.BreadCrumbs.withNamedViewAndScheduler("Brokers",schedulerName,"Stop Broker"),
        errorOrSuccess,
        "Stop Broker",
        FollowLink("Go to broker view.", scheduler.controllers.routes.SchedulerApplication.broker(schedulerName, id).toString()),
        FollowLink("Try again.", scheduler.controllers.routes.Broker.handleStopBroker(schedulerName, id).toString())
      ))
    }
  }

  def handleRemoveBroker(schedulerName: String, id: Int) = Action.async { implicit request =>
    kafkaManager.removeBroker(schedulerName, id).map { errorOrSuccess =>
      Ok(views.html.common.resultOfCommand(
        views.html.navigation.schedulerMenu(schedulerName, "Broker", "Broker View", Menus.schedulerMenus(schedulerName)),
        models.navigation.BreadCrumbs.withNamedViewAndScheduler("Brokers",schedulerName,"Remove Broker"),
        errorOrSuccess,
        "Remove Broker",
        FollowLink("Go to brokers list.", scheduler.controllers.routes.SchedulerApplication.brokers(schedulerName).toString()),
        FollowLink("Try again.", scheduler.controllers.routes.Broker.handleRemoveBroker(schedulerName, id).toString())
      ))
    }
  }

  def updateBroker(schedulerName: String, id: Int) = Action.async { implicit request =>
    val errorOrFormFuture = kafkaManager.getBrokerIdentity(schedulerName, id).map { errorOrBrokerIdentity =>
      errorOrBrokerIdentity.fold(e => -\/(e), { brokerIdentity: SchedulerBrokerIdentity =>
        \/-(updateBrokerForm(schedulerName, brokerIdentity))
      })
    }
    errorOrFormFuture.map { errorOrForm =>
      Ok(scheduler.views.html.broker.updateBroker(schedulerName, id, errorOrForm))
    }
  }

  def handleUpdateBroker(schedulerName: String, brokerId: Int) = Action.async { implicit request =>
    defaultUpdateForm.bindFromRequest.fold(
      formWithErrors => Future.successful(BadRequest(scheduler.views.html.broker.updateBroker(schedulerName, brokerId, \/-(formWithErrors)))),
      ub => {
        kafkaManager.updateBroker(schedulerName, ub.id, ub.cpus, ub.mem, ub.heap, ub.port, ub.bindAddress, ub.constraints, ub.options,
          ub.log4jOptions, ub.jvmOptions, ub.stickinessPeriod, ub.failover).map { errorOrSuccess =>
          Ok(views.html.common.resultOfCommand(
            views.html.navigation.schedulerMenu(schedulerName, "Broker", "Broker View", Menus.schedulerMenus(schedulerName)),
            models.navigation.BreadCrumbs.withNamedViewAndScheduler("Brokers",schedulerName,"Update Broker"),
            errorOrSuccess,
            "Update Broker",
            FollowLink("Go to broker view.",scheduler.controllers.routes.SchedulerApplication.broker(schedulerName, ub.id).toString()),
            FollowLink("Try again.",scheduler.controllers.routes.Broker.updateBroker(schedulerName, ub.id).toString())
          ))
        }
      }
    )
  }
}
