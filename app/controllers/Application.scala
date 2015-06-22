/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers

import play.api.mvc._

/**
 * @author hiral
 */
object Application extends Controller {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = KafkaManagerContext.getKafkaManager

  def index = Action.async {
    kafkaManager.getClusterList.map { errorOrClusterList =>
      Ok(views.html.index(errorOrClusterList))
    }
  }

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


}
