package scheduler.controllers

import controllers.KafkaManagerContext
import models.FollowLink
import scheduler.models.form.{RebalanceTopics => RebalanceTopicsOp}
import models.navigation.Menus
import play.api.data.Form
import play.api.data.Forms._
import play.api.mvc.{Action, Controller}

import scala.concurrent.Future
import scalaz.\/-

object RebalanceTopics extends Controller{
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = KafkaManagerContext.getKafkaManager

  val defaultRebalanceForm = Form(
    mapping(
      "ids" -> text,
      "topics" -> optional(text)
    )(RebalanceTopicsOp.apply)(RebalanceTopicsOp.unapply)
  )

  private val defaultF = RebalanceTopicsOp("*", Some(""))

  private val rebalanceForm = defaultRebalanceForm.fill(defaultF)

  def rebalanceTopics(schedulerName: String) = Action.async { implicit request =>
      Future.successful(Ok(scheduler.views.html.broker.rebalanceTopics(schedulerName, \/-(rebalanceForm))))
  }

  def handleRebalance(schedulerName: String) = Action.async { implicit request =>
    defaultRebalanceForm.bindFromRequest.fold(
      formWithErrors => Future.successful(BadRequest(scheduler.views.html.broker.rebalanceTopics(schedulerName,\/-(formWithErrors)))),
      rt => {
        kafkaManager.rebalanceTopics(schedulerName, rt.ids, rt.topics).map { errorOrSuccess =>
          Ok(views.html.common.resultOfCommand(
            views.html.navigation.schedulerMenu(schedulerName,"Rebalance Topics","",Menus.schedulerMenus(schedulerName)),
            models.navigation.BreadCrumbs.withNamedViewAndScheduler("Rebalance Topics",schedulerName,"Rebalance Topics"),
            errorOrSuccess,
            "Rebalance Topics",
            FollowLink("Go to the brokers list.", scheduler.controllers.routes.SchedulerApplication.brokers(schedulerName).toString()),
            FollowLink("Try again.",scheduler.controllers.routes.RebalanceTopics.rebalanceTopics(schedulerName).toString())
          ))
        }
      }
    )
  }
}
