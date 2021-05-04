/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package loader

import controllers.{AssetsComponents, BasicAuthenticationFilter, KafkaManagerContext}
import features.ApplicationFeatures
import models.navigation.Menus
import play.api.ApplicationLoader.Context
import play.api.i18n.I18nComponents
import play.api.mvc.Filter
import play.api.routing.Router
import play.api.{Application, ApplicationLoader, BuiltInComponentsFromContext, LoggerConfigurator}
import router.Routes

import scala.concurrent.ExecutionContext


/**
 * Created by hiral on 12/2/15.
 */
class KafkaManagerLoader extends ApplicationLoader {
  def load(context: Context): Application = {
    LoggerConfigurator(context.environment.classLoader).foreach {
      _.configure(context.environment, context.initialConfiguration, Map.empty)
    }
    new ApplicationComponents(context).application
  }
}

class ApplicationComponents(context: Context) extends BuiltInComponentsFromContext(context) with I18nComponents with AssetsComponents {
  implicit val applicationFeatures: ApplicationFeatures = ApplicationFeatures.getApplicationFeatures(context.initialConfiguration.underlying)
  implicit val menus: Menus = new Menus
  implicit val ec: ExecutionContext = controllerComponents.executionContext
  val kafkaManagerContext = new KafkaManagerContext(applicationLifecycle, context.initialConfiguration)
  private[this] val applicationC = new controllers.Application(controllerComponents, kafkaManagerContext)
  private[this] lazy val clusterC = new controllers.Cluster(controllerComponents, kafkaManagerContext)
  private[this] lazy val topicC = new controllers.Topic(controllerComponents, kafkaManagerContext)
  private[this] lazy val logKafkaC = new controllers.Logkafka(controllerComponents, kafkaManagerContext)
  private[this] lazy val consumerC = new controllers.Consumer(controllerComponents, kafkaManagerContext)
  private[this] lazy val preferredReplicaElectionC= new controllers.PreferredReplicaElection(controllerComponents, kafkaManagerContext)
  private[this] lazy val reassignPartitionsC = new controllers.ReassignPartitions(controllerComponents, kafkaManagerContext)
  lazy val kafkaStateCheckC = new controllers.api.KafkaStateCheck(controllerComponents, kafkaManagerContext)
  lazy val apiHealthC = new controllers.ApiHealth(controllerComponents)

  override lazy val httpFilters: Seq[Filter] = Seq(BasicAuthenticationFilter(context.initialConfiguration))


  override val router: Router = new Routes(
    httpErrorHandler, 
    applicationC, 
    clusterC, 
    topicC, 
    logKafkaC, 
    consumerC, 
    preferredReplicaElectionC,
    reassignPartitionsC, 
    kafkaStateCheckC, 
    assets,
    apiHealthC
  ).withPrefix(context.initialConfiguration.getOptional[String]("play.http.context").orNull)
}
