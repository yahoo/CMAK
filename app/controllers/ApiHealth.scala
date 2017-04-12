package controllers

import play.api.i18n.{I18nSupport, MessagesApi}
import play.api.mvc._

class ApiHealth(val messagesApi: MessagesApi) extends Controller with I18nSupport {

  def ping = Action {
    Ok("healthy")
  }

}
