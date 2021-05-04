package loader

import controllers.api.KafkaStateCheck
import controllers.{ApiHealth, KafkaManagerContext}
import features.ApplicationFeatures
import models.navigation.Menus
import play.api.ApplicationLoader.Context
import play.api.{Application, ApplicationLoader, LoggerConfigurator}

import scala.concurrent.ExecutionContext

class KafkaManagerLoaderForTests extends ApplicationLoader {
  var components: ApplicationComponents = null
  def applicationFeatures: ApplicationFeatures = components.applicationFeatures
  def menus: Menus = components.menus
  def executionContext: ExecutionContext = components.executionContext
  def kafkaManagerContext: KafkaManagerContext = components.kafkaManagerContext
  def kafkaStateCheck: KafkaStateCheck  = components.kafkaStateCheckC
  def apiHealth: ApiHealth= components.apiHealthC
  def load(context: Context): Application = {
    LoggerConfigurator(context.environment.classLoader).foreach {
      _.configure(context.environment, context.initialConfiguration, Map.empty)
    }
    components = new ApplicationComponents(context)
    components.application
  }
}
