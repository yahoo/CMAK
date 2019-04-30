package controllers

import play.api.i18n.I18nSupport
import play.api.mvc._

import scala.concurrent.ExecutionContext

class ApiHealth(val cc: ControllerComponents)(implicit ec:ExecutionContext) extends AbstractController(cc) with I18nSupport {

  def ping = Action { implicit request:RequestHeader =>
    Ok("healthy").withHeaders("X-Frame-Options" -> "SAMEORIGIN")
  }
}
