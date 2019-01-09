package controllers


import com.typesafe.config.ConfigValueType
import java.util.UUID

import com.unboundid.ldap.sdk._
import javax.net.ssl.SSLSocketFactory
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
import scala.util.{Success, Try}
import grizzled.slf4j.Logging

class BasicAuthenticationFilter(configurationFactory: => BasicAuthenticationFilterConfiguration) extends Filter with Logging {

  def apply(next: RequestHeader => Future[Result])(requestHeader: RequestHeader): Future[Result] =
    if (configuration.enabled && isNotExcluded(requestHeader))
      configuration.authenticator match {
        case authenticator: BasicAuthenticationFilterInternalAuthenticator =>
          checkInternalAuthentication(requestHeader, authenticator, next)
        case authenticator: BasicAuthenticationFilterLdapAuthenticator =>
          checkLdapAuthentication(requestHeader, authenticator, next)
      }
    else next(requestHeader)

  private def isNotExcluded(requestHeader: RequestHeader): Boolean =
    !configuration.excluded.exists( requestHeader.path matches _ )

  private def checkInternalAuthentication(requestHeader: RequestHeader, authenticator: BasicAuthenticationFilterInternalAuthenticator, next: RequestHeader => Future[Result]): Future[Result] =
    if (isAuthorized(requestHeader, authenticator)) addCookie(authenticator, next(requestHeader))
    else unauthorizedResult

  private def checkLdapAuthentication(requestHeader: RequestHeader, authenticator: BasicAuthenticationFilterLdapAuthenticator, next: RequestHeader => Future[Result]): Future[Result] = {
    val credentials = credentialsFromHeader(requestHeader)
    if (credentials.isDefined && isAuthorized(requestHeader, authenticator, credentials.get)) addCookie(credentials.get, next(requestHeader))
    else unauthorizedResult
  }

  private def isAuthorized(requestHeader: RequestHeader, authenticator: BasicAuthenticationFilterInternalAuthenticator) = {
    val expectedHeader = expectedHeaderValues(authenticator)
    lazy val authorizedByHeader =
      requestHeader.headers.get(AUTHORIZATION).exists(expectedHeader)

    val expectedCookie = cookieValue(authenticator)
    lazy val authorizedByCookie =
      requestHeader.cookies.get(COOKIE_NAME).exists(_.value == expectedCookie)

    authorizedByHeader || authorizedByCookie
  }

  private def isAuthorized(requestHeader: RequestHeader, authenticator: BasicAuthenticationFilterLdapAuthenticator, credentials: (String, String)) = {
    val (username, password) = credentials
    val expectedCookie = cookieValue(username, Set(password))
    val authorizedByCookie =
      requestHeader.cookies.get(COOKIE_NAME).exists(_.value == expectedCookie)

    authorizedByCookie || {
      val connection = ldapConnectionPool.getConnection
      try {
        findUserDN(authenticator.searchBaseDN, authenticator.searchFilter, username, connection) match {
          case None => {
            logger.debug(s"Can't find user DN for username: $username. " +
              s"Base DN: ${authenticator.searchBaseDN}. " +
              s"Filter: ${renderSearchFilter(authenticator.searchFilter, username)}")
            false
          }
          case Some(userDN) => Try(connection.bind(userDN, password)).isSuccess
        }
      } finally {
        connection.close()
      }
    }
  }

  private def findUserDN(baseDN: String, filterTemplate: String, username: String, connection: LDAPConnection) = {
    val filter = renderSearchFilter(filterTemplate, username)
    val searchRequest = new SearchRequest(baseDN, SearchScope.SUB, filter)
    Try(connection.search(searchRequest)) match {
      case Success(sr) if sr.getEntryCount > 0 => Some(sr.getSearchEntries.get(0).getDN)
      case _ => None
    }
  }

  private def renderSearchFilter(filterTemplate: String, username: String) = {
    filterTemplate.replaceAll("\\$capturedLogin\\$", username)
  }

  private def addCookie(authenticator: BasicAuthenticationFilterInternalAuthenticator, result: Future[Result]) =
    result.map(_.withCookies(cookie(authenticator)))

  private def addCookie(credentials: (String, String), result: Future[Result]) = {
    val (username, password) = credentials
    result.map(_.withCookies(cookie(username, password)))
  }

  private lazy val configuration = configurationFactory

  private lazy val unauthorizedResult =
    Future successful Unauthorized.withHeaders(WWW_AUTHENTICATE -> realm)

  private lazy val COOKIE_NAME = "play-basic-authentication-filter"

  private lazy val ldapConnectionPool: LDAPConnectionPool = {
    val authenticator = configuration.authenticator.asInstanceOf[BasicAuthenticationFilterLdapAuthenticator]
    val (address, port) = (authenticator.address, authenticator.port)
    val connection = if (authenticator.sslEnabled) {
      new LDAPConnection(SSLSocketFactory.getDefault, address, port, authenticator.username, authenticator.password)
    } else {
      new LDAPConnection(address, port, authenticator.username, authenticator.password)
    }
    new LDAPConnectionPool(connection, authenticator.connectionPoolSize)
  }

  private def cookie(configuration: BasicAuthenticationFilterInternalAuthenticator) = Cookie(COOKIE_NAME, cookieValue(configuration))
  private def cookie(username: String, password: String) = Cookie(COOKIE_NAME, cookieValue(username, Set(password)))

  private def cookieValue(configuration: BasicAuthenticationFilterInternalAuthenticator): String =
    cookieValue(configuration.username, configuration.passwords)

  private def cookieValue(username: String, passwords: Set[String]): String =
    Crypto.sign(username + passwords.mkString(","))

  private def expectedHeaderValues(configuration: BasicAuthenticationFilterInternalAuthenticator) =
    configuration.passwords.map { password =>
      val combined = configuration.username + ":" + password
      val credentials = Base64.encodeBase64String(combined.getBytes)
      basic(credentials)
    }

  private def credentialsFromHeader(requestHeader: RequestHeader): Option[(String, String)] = {
    requestHeader.headers.get(AUTHORIZATION).flatMap(authorization => {
      authorization.split("\\s+").toList match {
        case "Basic" :: base64Hash :: Nil => {
          val credentials = new String(org.apache.commons.codec.binary.Base64.decodeBase64(base64Hash.getBytes))
          credentials.split(":").toList match {
            case username :: password :: Nil => Some(username -> password)
            case _ => None
          }
        }
        case _ => None
      }
    })
  }

  private lazy val realm = basic(s"""realm=\"${configuration.realm}"""")

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

case class BasicAuthenticationFilterConfiguration(realm: String,
                                                  enabled: Boolean,
                                                  authenticator: BasicAuthenticationFilterAuthenticator,
                                                  excluded: Set[String])

sealed trait BasicAuthenticationFilterAuthenticator

case class BasicAuthenticationFilterInternalAuthenticator(username: String,
                                                          passwords: Set[String]) extends BasicAuthenticationFilterAuthenticator

case class BasicAuthenticationFilterLdapAuthenticator(address: String,
                                                      port: Int,
                                                      username: String,
                                                      password: String,
                                                      searchBaseDN: String,
                                                      searchFilter: String,
                                                      connectionPoolSize: Int,
                                                      sslEnabled: Boolean) extends BasicAuthenticationFilterAuthenticator


object BasicAuthenticationFilterConfiguration {

  private val defaultRealm = "Application"
  private def credentialsMissingRealm(realm: String) =
    s"$realm: The username or password could not be found in the configuration."

  def parse(configuration: Configuration) = {

    val root = "basicAuthentication."
    def boolean(key: String) = configuration.getBoolean(root + key)
    def string(key: String) = configuration.getString(root + key)
    def int(key: String) = configuration.getInt(root + key)
    def seq(key: String) =
      Option(configuration.underlying getValue (root + key)).map { value =>
        value.valueType match {
          case ConfigValueType.LIST => value.unwrapped.asInstanceOf[java.util.List[String]].asScala
          case ConfigValueType.STRING => Seq(value.unwrapped.asInstanceOf[String])
          case _ => sys.error(s"Unexpected value at `${root + key}`, expected STRING or LIST")
        }
      }

    val enabled = boolean("enabled").getOrElse(false)
    val ldapEnabled = boolean("ldap.enabled").getOrElse(false)

    val excluded = configuration.getStringSeq(root + "excluded")
      .getOrElse(Seq.empty)
      .toSet

    if (ldapEnabled) {
      val connection: Option[(String, Int)] = for {
        server <- string("ldap.server")
        port <- int("ldap.port")
      } yield (server, port)

      val (server, port) = {
        connection.getOrElse(("localhost", 389))
      }

      val username = string("ldap.username").getOrElse("")
      val password = string("ldap.password").getOrElse("")

      val searchDN = string("ldap.search-base-dn").getOrElse("")
      val searchFilter = string("ldap.search-filter").getOrElse("")
      val connectionPoolSize = int("ldap.connection-pool-size").getOrElse(10)
      val sslEnabled = boolean("ldap.ssl").getOrElse(false)

      BasicAuthenticationFilterConfiguration(
        string("realm").getOrElse(defaultRealm),
        enabled,
        BasicAuthenticationFilterLdapAuthenticator(
          server, port, username, password, searchDN, searchFilter, connectionPoolSize, sslEnabled
        ),
        excluded
      )
    } else {
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

      BasicAuthenticationFilterConfiguration(
        realm(credentials.isDefined),
        enabled,
        BasicAuthenticationFilterInternalAuthenticator(username, passwords),
        excluded
      )
    }

  }
}