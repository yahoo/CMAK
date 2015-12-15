package scheduler.controllers

import controllers.KafkaManagerContext
import play.api.mvc.{Action, Controller}

object SchedulerApplication extends Controller {

  private[this] val kafkaManager = KafkaManagerContext.getKafkaManager

  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  def getScheduler(s: String) = Action.async {
    kafkaManager.getSchedulerView(s).map { errorOrSchedulerView =>
      Ok(scheduler.views.html.scheduler.schedulerView(s,errorOrSchedulerView))
    }
  }

  def brokers(s: String) = Action.async {
    kafkaManager.getSchedulerBrokerList(s).map { errorOrBrokerList =>
      Ok(scheduler.views.html.broker.brokerList(s,errorOrBrokerList))
    }
  }

  def broker(s: String, b: Int) = Action.async {
    kafkaManager.getBrokerIdentity(s,b).map { errorOrBrokerView =>
      Ok(scheduler.views.html.broker.brokerView(s,b,errorOrBrokerView))
    }
  }
}

