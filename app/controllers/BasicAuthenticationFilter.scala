package controllers


import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.util.UUID
import akka.stream.Materializer
import com.typesafe.config.ConfigValueType
import com.unboundid.ldap.sdk._
import com.unboundid.ldap.sdk.extensions.StartTLSExtendedRequest
import com.unboundid.util.ssl.{SSLUtil, TrustAllTrustManager}
import grizzled.slf4j.Logging

import javax.crypto.Mac
import javax.net.ssl
import org.apache.commons.codec.binary.Base64
import play.api.Configuration
import play.api.http.HeaderNames.{AUTHORIZATION, WWW_AUTHENTICATE}
import play.api.libs.Codecs
import play.api.mvc.Results.Unauthorized
import play.api.mvc.{Cookie, Filter, RequestHeader, Result}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

class BasicAuthenticationFilter(configuration: BasicAuthenticationFilterConfiguration, authenticator: Authenticator)(implicit val mat: Materializer, ec: ExecutionContext) extends Filter {

  def apply(next: RequestHeader => Future[Result])(requestHeader: RequestHeader): Future[Result] =
    if (configuration.enabled && isNotExcluded(requestHeader)) {
      authenticator.checkAuthentication(requestHeader, next)
    }
    else next(requestHeader)

  private def isNotExcluded(requestHeader: RequestHeader): Boolean =
    !configuration.excluded.exists(requestHeader.path matches _)

}

trait Authenticator {

  import javax.crypto.spec.{IvParameterSpec, PBEKeySpec, SecretKeySpec}
  import javax.crypto.{Cipher, SecretKeyFactory}

  private lazy val factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
  private lazy val spec = new PBEKeySpec(secret, salt, 65536, 256)
  private lazy val secretKey = new SecretKeySpec(factory.generateSecret(spec).getEncoded, "AES")
  private lazy val cipher: Cipher = {
    val c = Cipher.getInstance("AES/CBC/PKCS5Padding")
    c.init(Cipher.ENCRYPT_MODE, secretKey, new IvParameterSpec(iv))
    c
  }

  private lazy val mac: Mac = {
    val m = Mac.getInstance("HmacSHA256")
    m.init(new SecretKeySpec(factory.generateSecret(spec).getEncoded, "HmacSHA256"))
    m
  }

  def salt: Array[Byte]

  def iv: Array[Byte]

  def secret: Array[Char]

  def encrypt(content: Array[Byte]): Array[Byte] = {
    cipher.doFinal(content)
  }

  def decrypt(content: Array[Byte], iv: Array[Byte]): Array[Byte] = {
    cipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(iv))
    cipher.doFinal(content)
  }

  def sign(content: String): String = {
    Codecs.toHexString(mac.doFinal(content.getBytes(StandardCharsets.UTF_8)))
  }

  def checkAuthentication(requestHeader: RequestHeader, next: RequestHeader => Future[Result]): Future[Result]
}

object BasicAuthenticator {
  private lazy val COOKIE_NAME = "play-basic-authentication"
}

case class BasicAuthenticator(config: BasicAuthenticationConfig)(implicit val mat: Materializer, ec: ExecutionContext) extends Authenticator {

  import BasicAuthenticator._

  private lazy val realm = basic(s"""realm="${config.realm}"""")
  private lazy val unauthorizedResult = Future successful Unauthorized.withHeaders(WWW_AUTHENTICATE -> realm)

  def salt: Array[Byte] = config.salt

  def iv: Array[Byte] = config.iv

  def secret: Array[Char] = config.secret

  def checkAuthentication(requestHeader: RequestHeader, next: RequestHeader => Future[Result]): Future[Result] = {
    if (isAuthorized(requestHeader)) addCookie(next(requestHeader))
    else unauthorizedResult
  }

  private def addCookie(result: Future[Result]) =
    result.map(_.withCookies(cookie))

  private def isAuthorized(requestHeader: RequestHeader) = {
    val expectedHeader = expectedHeaderValues(config)
    val authorizedByHeader = requestHeader.headers.get(AUTHORIZATION).exists(expectedHeader)

    val expectedCookie = cookieValue
    val authorizedByCookie = requestHeader.cookies.get(COOKIE_NAME).exists(_.value == expectedCookie)

    authorizedByHeader || authorizedByCookie
  }

  private def cookie = Cookie(COOKIE_NAME, cookieValue, maxAge = Option(3600))

  private lazy val cookieValue: String =
    cookieValue(config.username, config.passwords)

  private def cookieValue(username: String, passwords: Set[String]): String =
    new String(Base64.encodeBase64((username + passwords.mkString(",")).getBytes(StandardCharsets.UTF_8)))

  private def expectedHeaderValues(configuration: BasicAuthenticationConfig) =
    configuration.passwords.map { password =>
      val combined = configuration.username + ":" + password
      val credentials = Base64.encodeBase64String(combined.getBytes)
      basic(credentials)
    }

  private def basic(content: String) = s"Basic $content"
}

object LDAPAuthenticator {
  private lazy val COOKIE_NAME = "play-basic-ldap-authentication"
}

case class LDAPAuthenticator(config: LDAPAuthenticationConfig)(implicit val mat: Materializer, ec: ExecutionContext) extends Authenticator with Logging {

  import LDAPAuthenticator._

  private lazy val realm = basic(s"""realm="${config.realm}"""")
  private lazy val unauthorizedResult = Future successful Unauthorized.withHeaders(WWW_AUTHENTICATE -> realm)
  private lazy val ldapConnectionPool: LDAPConnectionPool = {
    val (address, port) = (config.address, config.port)

    if (config.sslEnabled && config.startTLSEnabled) {
      logger.error("SSL and StartTLS enabled together. Most LDAP Server implementations will not handle this as it initializes an encrypted context over an already encrypted channel")
    }

    val connection = if (config.sslEnabled) {
      if (config.sslTrustAll) {
        val sslUtil = new SSLUtil(null, new TrustAllTrustManager(true))
        val sslSocketFactory = sslUtil.createSSLSocketFactory
        new LDAPConnection(sslSocketFactory, address, port)
      } else {
        val sslSocketFactory = ssl.SSLSocketFactory.getDefault
        new LDAPConnection(sslSocketFactory, address, port)
      }
    } else {
      new LDAPConnection(address, port)
    }

    var startTLSPostConnectProcessor : StartTLSPostConnectProcessor = null
    if (config.startTLSEnabled) {
      if (config.sslTrustAll) {
        val sslUtil = new SSLUtil(null, new TrustAllTrustManager(true))
        val sslContext = sslUtil.createSSLContext
        connection.processExtendedOperation(new StartTLSExtendedRequest(sslContext))
        startTLSPostConnectProcessor = new StartTLSPostConnectProcessor(sslContext)
      } else {
        val sslContext = new SSLUtil().createSSLContext
        connection.processExtendedOperation(new StartTLSExtendedRequest(sslContext))
        startTLSPostConnectProcessor = new StartTLSPostConnectProcessor(sslContext)
      }
    }

    try {
      connection.bind(config.username, config.password)
    } catch {
      case e: LDAPException => {
        connection.setDisconnectInfo(DisconnectType.BIND_FAILED, null, e)
        connection.close()
        logger.error(s"Bind failed with ldap server ${config.address}:${config.port}", e)
      }
    }

    new LDAPConnectionPool(connection, 1, config.connectionPoolSize, startTLSPostConnectProcessor)
  }

  def salt: Array[Byte] = config.salt

  def iv: Array[Byte] = config.iv

  def secret: Array[Char] = config.secret

  def checkAuthentication(requestHeader: RequestHeader, next: RequestHeader => Future[Result]): Future[Result] = {
    val credentials = credentialsFromHeader(requestHeader)
    if (credentials.isDefined && isAuthorized(requestHeader, credentials.get)) addCookie(credentials.get, next(requestHeader))
    else unauthorizedResult
  }

  private def credentialsFromHeader(requestHeader: RequestHeader): Option[(String, String)] = {
    requestHeader.headers.get(AUTHORIZATION).flatMap(authorization => {
      authorization.split("\\s+").toList match {
        case "Basic" :: base64Hash :: Nil => {
          val credentials = new String(org.apache.commons.codec.binary.Base64.decodeBase64(base64Hash.getBytes))
          credentials.split(":", 2).toList match {
            case username :: password :: Nil => Some(username -> password)
            case _ => None
          }
        }
        case _ => None
      }
    })
  }

  private def isAuthorized(requestHeader: RequestHeader, credentials: (String, String)) = {
    val (username, password) = credentials
    val expectedCookie = cookieValue(username, Set(password))
    val authorizedByCookie =
      requestHeader.cookies.get(COOKIE_NAME).exists(_.value == expectedCookie)

    authorizedByCookie || {
      val connection = ldapConnectionPool.getConnection
      try {
        findUserDN(config.searchBaseDN, config.searchFilter, username, connection) match {
          case None =>
            logger.debug(s"Can't find user DN for username: $username. " +
              s"Base DN: ${config.searchBaseDN}. " +
              s"Filter: ${renderSearchFilter(config.searchFilter, username)}")
            false
          case Some(userDN) =>
            //Check if user is in specified group
            if (!config.groupFilter.isEmpty) {
              val compareResult = connection.compare(new CompareRequest(userDN, "memberOf", config.groupFilter))
              if (compareResult.compareMatched()) {
                Try(connection.bind(userDN, password)).isSuccess
              } else {
                logger.debug(s"User $username is not member of Group ${config.groupFilter}")
                false
              }
            } else {
              Try(connection.bind(userDN, password)).isSuccess
            }
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

  private def addCookie(credentials: (String, String), result: Future[Result]) = {
    val (username, password) = credentials
    result.map(_.withCookies(cookie(username, password)))
  }

  private def cookieValue(username: String, passwords: Set[String]): String =
    sign(username + passwords.mkString(","))

  private def basic(content: String) = s"Basic $content"

  private def cookie(username: String, password: String) = Cookie(COOKIE_NAME, cookieValue(username, Set(password)), maxAge = Option(3600))
}

sealed trait AuthenticationConfig {
  def salt: Array[Byte]

  def iv: Array[Byte]

  def secret: Array[Char]
}

case class BasicAuthenticationConfig(salt: Array[Byte]
                                     , iv: Array[Byte]
                                     , secret: Array[Char]
                                     , realm: String
                                     , username: String
                                     , passwords: Set[String]) extends AuthenticationConfig

case class LDAPAuthenticationConfig(salt: Array[Byte]
                                    , iv: Array[Byte]
                                    , secret: Array[Char]
                                    , realm: String
                                    , address: String
                                    , port: Int
                                    , username: String
                                    , password: String
                                    , searchBaseDN: String
                                    , searchFilter: String
                                    , groupFilter: String
                                    , connectionPoolSize: Int
                                    , sslEnabled: Boolean
                                    , sslTrustAll: Boolean
                                    , startTLSEnabled: Boolean) extends AuthenticationConfig

sealed trait AuthType[T <: AuthenticationConfig] {
  def getConfig(config: AuthenticationConfig): T
}

case object BasicAuth extends AuthType[BasicAuthenticationConfig] {
  def getConfig(config: AuthenticationConfig): BasicAuthenticationConfig = {
    require(config.isInstanceOf[BasicAuthenticationConfig], s"Unexpected config type : ${config.getClass.getSimpleName}")
    config.asInstanceOf[BasicAuthenticationConfig]
  }
}

case object LDAPAuth extends AuthType[LDAPAuthenticationConfig] {
  def getConfig(config: AuthenticationConfig): LDAPAuthenticationConfig = {
    require(config.isInstanceOf[LDAPAuthenticationConfig], s"Unexpected config type : ${config.getClass.getSimpleName}")
    config.asInstanceOf[LDAPAuthenticationConfig]
  }
}

case class BasicAuthenticationFilterConfiguration(enabled: Boolean,
                                                  authType: AuthType[_ <: AuthenticationConfig],
                                                  authenticationConfig: AuthenticationConfig,
                                                  excluded: Set[String])

object BasicAuthenticationFilterConfiguration {

  private val SALT_LEN = 20
  private val defaultRealm = "Application"

  private def credentialsMissingRealm(realm: String) =
    s"$realm: The username or password could not be found in the configuration."

  def parse(configuration: Configuration): BasicAuthenticationFilterConfiguration = {

    val root = "basicAuthentication."

    def boolean(key: String) = configuration.getOptional[Boolean](root + key)

    def string(key: String) = configuration.getOptional[String](root + key)

    def int(key: String) = configuration.getOptional[Int](root + key)

    def seq(key: String) =
      Option(configuration.underlying getValue (root + key)).map { value =>
        value.valueType match {
          case ConfigValueType.LIST => value.unwrapped.asInstanceOf[java.util.List[String]].asScala
          case ConfigValueType.STRING => Seq(value.unwrapped.asInstanceOf[String])
          case _ => sys.error(s"Unexpected value at `${root + key}`, expected STRING or LIST")
        }
      }

    val sr = new SecureRandom()
    val salt: Array[Byte] = string("salt").map(Codecs.hexStringToByte).getOrElse(sr.generateSeed(SALT_LEN))
    val iv: Array[Byte] = string("iv").map(Codecs.hexStringToByte).getOrElse(sr.generateSeed(SALT_LEN))
    val secret: Array[Char] = string("secret").map(_.toCharArray).getOrElse(UUID.randomUUID().toString.toCharArray)
    val enabled = boolean("enabled").getOrElse(false)
    val ldapEnabled = boolean("ldap.enabled").getOrElse(false)

    val excluded = configuration.getOptional[Seq[String]](root + "excluded")
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
      val groupFilter = string("ldap.group-filter").getOrElse("")
      val connectionPoolSize = int("ldap.connection-pool-size").getOrElse(10)
      val sslEnabled = boolean("ldap.ssl").getOrElse(false)
      val sslTrustAll = boolean("ldap.ssl-trust-all").getOrElse(false)
      val startTLSEnabled = boolean("ldap.starttls").getOrElse(false)

      BasicAuthenticationFilterConfiguration(
        enabled,
        LDAPAuth,
        LDAPAuthenticationConfig(salt, iv, secret,
          string("realm").getOrElse(defaultRealm),
          server, port, username, password, searchDN, searchFilter, groupFilter, connectionPoolSize, sslEnabled, sslTrustAll, startTLSEnabled
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
        enabled,
        BasicAuth,
        BasicAuthenticationConfig(salt, iv, secret, realm(credentials.isDefined), username, passwords),
        excluded
      )
    }

  }
}

object BasicAuthenticationFilter {
  def apply(configuration: => Configuration)(implicit mat: Materializer, ec: ExecutionContext): Filter = {
    val filterConfig = BasicAuthenticationFilterConfiguration.parse(configuration)
    val authenticator = filterConfig.authType match {
      case BasicAuth =>
        new BasicAuthenticator(BasicAuth.getConfig(filterConfig.authenticationConfig))
      case LDAPAuth =>
        new LDAPAuthenticator(LDAPAuth.getConfig(filterConfig.authenticationConfig))
    }
    new BasicAuthenticationFilter(filterConfig, authenticator)
  }
}