package controllers


import com.typesafe.config.ConfigValueType

import java.util.UUID

import org.apache.commons.codec.binary.Base64

import play.api.Configuration
import play.api.http.HeaderNames.AUTHORIZATION
import play.api.http.HeaderNames.WWW_AUTHENTICATE
import play.api.libs.Crypto
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.mvc.Cookie
import play.api.mvc.Filter
import play.api.mvc.RequestHeader
import play.api.mvc.Result
import play.api.mvc.Results.Unauthorized

import scala.collection.JavaConverters._
import scala.concurrent.Future

class BasicAuthenticationFilter(configurationFactory: => BasicAuthenticationFilterConfiguration) extends Filter {

  def apply(next: RequestHeader => Future[Result])(requestHeader: RequestHeader): Future[Result] =
    if (configuration.enabled && isNotExcluded(requestHeader))
      checkAuthentication(requestHeader, next)
    else next(requestHeader)

  private def isNotExcluded(requestHeader: RequestHeader): Boolean =
    !configuration.excluded.exists( requestHeader.path matches _ )

  private def checkAuthentication(requestHeader: RequestHeader, next: RequestHeader => Future[Result]): Future[Result] =
    if (isAuthorized(requestHeader)) addCookie(next(requestHeader))
    else unauthorizedResult

  private def isAuthorized(requestHeader: RequestHeader) = {
    lazy val authorizedByHeader =
      requestHeader.headers.get(AUTHORIZATION).exists(expectedHeaderValues)

    lazy val authorizedByCookie =
      requestHeader.cookies.get(COOKIE_NAME).exists(_.value == cookieValue)

    authorizedByHeader || authorizedByCookie
  }

  private def addCookie(result: Future[Result]) =
    result.map(_.withCookies(cookie))

  private lazy val configuration = configurationFactory

  private lazy val unauthorizedResult =
    Future successful Unauthorized.withHeaders(WWW_AUTHENTICATE -> realm)

  private lazy val COOKIE_NAME = "play-basic-authentication-filter"

  private lazy val cookie = Cookie(COOKIE_NAME, cookieValue)

  private lazy val cookieValue =
    Crypto.sign(configuration.username + configuration.passwords)

  private lazy val expectedHeaderValues =
    configuration.passwords.map { password =>
      val combined = configuration.username + ":" + password
      val credentials = Base64.encodeBase64String(combined.getBytes)
      basic(credentials)
    }

  private def realm = basic(s"""realm=\"${configuration.realm}"""")

  private def basic(content: String) = s"Basic $content"
}

object BasicAuthenticationFilter {
  def apply() = new BasicAuthenticationFilter(
    BasicAuthenticationFilterConfiguration.parse(
      play.api.Play.current.configuration
    )
  )

  def apply(configuration: => Configuration) = new BasicAuthenticationFilter(
    BasicAuthenticationFilterConfiguration parse configuration
  )
}

case class BasicAuthenticationFilterConfiguration(
                                                   realm: String,
                                                   enabled: Boolean,
                                                   username: String,
                                                   passwords: Set[String],
                                                   excluded: Set[String])

object BasicAuthenticationFilterConfiguration {

  private val defaultRealm = "Application"
  private def credentialsMissingRealm(realm: String) =
    s"$realm: The username or password could not be found in the configuration."

  def parse(configuration: Configuration) = {

    val root = "basicAuthentication."
    def boolean(key: String) = configuration.getBoolean(root + key)
    def string(key: String) = configuration.getString(root + key)
    def seq(key: String) =
      Option(configuration.underlying getValue (root + key)).map { value =>
        value.valueType match {
          case ConfigValueType.LIST => value.unwrapped.asInstanceOf[java.util.List[String]].asScala
          case ConfigValueType.STRING => Seq(value.unwrapped.asInstanceOf[String])
          case _ => sys.error(s"Unexpected value at `${root + key}`, expected STRING or LIST")
        }
      }

    val enabled = boolean("enabled").getOrElse(false)

    val credentials: Option[(String, Set[String])] = for {
      username <- string("username")
      passwords <- seq("password")
    } yield (username, passwords.toSet)

    val (username, passwords) = {
      def uuid = UUID.randomUUID.toString
      credentials.getOrElse((uuid, Set(uuid)))
    }

    def realm(hasCredentials: Boolean) = {
      val realm = string("realm").getOrElse(defaultRealm)
      if (hasCredentials) realm
      else credentialsMissingRealm(realm)
    }

    val excluded = configuration.getStringSeq(root + "excluded")
      .getOrElse(Seq.empty)
      .toSet

    BasicAuthenticationFilterConfiguration(
      realm(credentials.isDefined),
      enabled,
      username,
      passwords,
      excluded
    )
  }
}