/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.model

import java.nio.charset.StandardCharsets

import kafka.manager.features.ClusterFeatures

import scala.util.Try
import scala.util.matching.Regex
import scalaz.Validation.FlatMap._

/**
  * @author hiral
  */
case class CuratorConfig(zkConnect: String, zkMaxRetry: Int = 100, baseSleepTimeMs : Int = 100, maxSleepTimeMs: Int = 1000)

sealed trait KafkaVersion
case object Kafka_0_8_1_1 extends KafkaVersion {
  override def toString = "0.8.1.1"
}
case object Kafka_0_8_2_0 extends KafkaVersion {
  override def toString = "0.8.2.0"
}
case object Kafka_0_8_2_1 extends KafkaVersion {
  override def toString = "0.8.2.1"
}
case object Kafka_0_8_2_2 extends KafkaVersion {
  override def toString = "0.8.2.2"
}
case object Kafka_0_9_0_0 extends KafkaVersion {
  override def toString = "0.9.0.0"
}
case object Kafka_0_9_0_1 extends KafkaVersion {
  override def toString = "0.9.0.1"
}
case object Kafka_0_10_0_0 extends KafkaVersion {
  override def toString = "0.10.0.0"
}
case object Kafka_0_10_0_1 extends KafkaVersion {
  override def toString = "0.10.0.1"
}

case object Kafka_0_10_1_0 extends KafkaVersion {
  override def toString = "0.10.1.0"
}

case object Kafka_0_10_1_1 extends KafkaVersion {
  override def toString = "0.10.1.1"
}

case object Kafka_0_10_2_0 extends KafkaVersion {
  override def toString = "0.10.2.0"
}

case object Kafka_0_10_2_1 extends KafkaVersion {
  override def toString = "0.10.2.1"
}

case object Kafka_0_11_0_0 extends KafkaVersion {
  override def toString = "0.11.0.0"
}

object KafkaVersion {
  val supportedVersions: Map[String,KafkaVersion] = Map(
    "0.8.1.1" -> Kafka_0_8_1_1,
    "0.8.2-beta" -> Kafka_0_8_2_0,
    "0.8.2.0" -> Kafka_0_8_2_0,
    "0.8.2.1" -> Kafka_0_8_2_1,
    "0.8.2.2" -> Kafka_0_8_2_2,
    "0.9.0.0" -> Kafka_0_9_0_0,
    "0.9.0.1" -> Kafka_0_9_0_1,
    "0.10.0.0" -> Kafka_0_10_0_0,
    "0.10.0.1" -> Kafka_0_10_0_1,
    "0.10.1.0" -> Kafka_0_10_1_0,
    "0.10.1.1" -> Kafka_0_10_1_1,
    "0.10.2.0" -> Kafka_0_10_2_0,
    "0.10.2.1" -> Kafka_0_10_2_1,
    "0.11.0.0" -> Kafka_0_11_0_0
  )

  val formSelectList : IndexedSeq[(String,String)] = supportedVersions.toIndexedSeq.filterNot(_._1.contains("beta")).map(t => (t._1,t._2.toString)).sortWith((a, b) => sortVersion(a._1, b._1))

  def apply(s: String) : KafkaVersion = {
    supportedVersions.get(s) match {
      case Some(v) => v
      case None => throw new IllegalArgumentException(s"Unsupported kafka version : $s")
    }
  }

  def unapply(v: KafkaVersion) : Option[String] = {
    Some(v.toString)
  }

  private def sortVersion(versionNum: String, kafkaVersion: String): Boolean = {
    val separator = "\\."
    val versionNumList = versionNum.split(separator, -1).toList
    val kafkaVersionList = kafkaVersion.split(separator, -1).toList
    def compare(a: List[String], b: List[String]): Boolean = a.nonEmpty match {
      case true if b.nonEmpty =>
        if (a.head == b.head) compare(a.tail, b.tail) else a.head.toInt < b.head.toInt
      case true if b.isEmpty => false
      case false if b.nonEmpty => true
      case _ => true
    }
    compare(versionNumList, kafkaVersionList)
  }
}

object ClusterConfig {
  val legalChars = "[a-zA-Z0-9\\._\\-]"
  private val maxNameLength = 255
  val regex = new Regex(legalChars + "+")

  def validateName(clusterName: String) {
    require(clusterName.length > 0, "cluster name is illegal, can't be empty")
    require(!(clusterName.equals(".") || clusterName.equals("..")), "cluster name cannot be \".\" or \"..\"")
    require(clusterName.length <= maxNameLength,"cluster name is illegal, can't be longer than " + maxNameLength + " characters")
    regex.findFirstIn(clusterName) match {
      case Some(t) =>
        require(t.equals(clusterName),
          ("cluster name " + clusterName + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'"))
      case None =>
        require(false,"cluster name " + clusterName + " is illegal,  contains a character other than ASCII alphanumerics, '.', '_' and '-'")
    }
  }

  def validateZkHosts(zkHosts: String): Unit = {
    require(zkHosts.length > 0, "cluster zk hosts is illegal, can't be empty!")
  }

  def apply(name: String
            , version : String
            , zkHosts: String
            , zkMaxRetry: Int = 100
            , jmxEnabled: Boolean
            , jmxUser: Option[String]
            , jmxPass: Option[String]
            , jmxSsl: Boolean
            , pollConsumers: Boolean
            , filterConsumers: Boolean
            , logkafkaEnabled: Boolean = false
            , activeOffsetCacheEnabled: Boolean = false
            , displaySizeEnabled: Boolean = false
            , tuning: Option[ClusterTuning]
            , securityProtocol: String
           ) : ClusterConfig = {
    val kafkaVersion = KafkaVersion(version)
    //validate cluster name
    validateName(name)
    //validate zk hosts
    validateZkHosts(zkHosts)
    val cleanZkHosts = zkHosts.replaceAll(" ","")
    new ClusterConfig(
      name
      , CuratorConfig(cleanZkHosts, zkMaxRetry)
      , true
      , kafkaVersion
      , jmxEnabled
      , jmxUser
      , jmxPass
      , jmxSsl
      , pollConsumers
      , filterConsumers
      , logkafkaEnabled
      , activeOffsetCacheEnabled
      , displaySizeEnabled
      , tuning
      , SecurityProtocol(securityProtocol)
    )
  }

  def customUnapply(cc: ClusterConfig) : Option[(
    String, String, String, Int, Boolean, Option[String], Option[String], Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Option[ClusterTuning], String)] = {
    Some((
      cc.name, cc.version.toString, cc.curatorConfig.zkConnect, cc.curatorConfig.zkMaxRetry,
      cc.jmxEnabled, cc.jmxUser, cc.jmxPass, cc.jmxSsl, cc.pollConsumers, cc.filterConsumers,
      cc.logkafkaEnabled, cc.activeOffsetCacheEnabled, cc.displaySizeEnabled, cc.tuning, cc.securityProtocol.stringId
      )
    )
  }

  import scalaz.syntax.applicative._
  import scalaz.{Failure, Success}
  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  import org.json4s.jackson.Serialization
  import org.json4s.scalaz.JsonScalaz._

  import scala.language.reflectiveCalls

  implicit val formats = Serialization.formats(FullTypeHints(List(classOf[ClusterConfig])))

  implicit def curatorConfigJSONW: JSONW[CuratorConfig] = new JSONW[CuratorConfig] {
    def write(a: CuratorConfig) =
      makeObj(("zkConnect" -> toJSON(a.zkConnect))
        :: ("zkMaxRetry" -> toJSON(a.zkMaxRetry))
        :: ("baseSleepTimeMs" -> toJSON(a.baseSleepTimeMs))
        :: ("maxSleepTimeMs" -> toJSON(a.maxSleepTimeMs))
        :: Nil)
  }

  implicit def curatorConfigJSONR: JSONR[CuratorConfig] = CuratorConfig.applyJSON(
    fieldExtended[String]("zkConnect"), fieldExtended[Int]("zkMaxRetry"), fieldExtended[Int]("baseSleepTimeMs"), fieldExtended[Int]("maxSleepTimeMs"))

  def serialize(config: ClusterConfig) : Array[Byte] = {
    val json = makeObj(("name" -> toJSON(config.name))
      :: ("curatorConfig" -> toJSON(config.curatorConfig))
      :: ("enabled" -> toJSON(config.enabled))
      :: ("kafkaVersion" -> toJSON(config.version.toString))
      :: ("jmxEnabled" -> toJSON(config.jmxEnabled))
      :: ("jmxUser" -> toJSON(config.jmxUser))
      :: ("jmxPass" -> toJSON(config.jmxPass))
      :: ("jmxSsl" -> toJSON(config.jmxSsl))
      :: ("pollConsumers" -> toJSON(config.pollConsumers))
      :: ("filterConsumers" -> toJSON(config.filterConsumers))
      :: ("logkafkaEnabled" -> toJSON(config.logkafkaEnabled))
      :: ("activeOffsetCacheEnabled" -> toJSON(config.activeOffsetCacheEnabled))
      :: ("displaySizeEnabled" -> toJSON(config.displaySizeEnabled))
      :: ("tuning" -> toJSON(config.tuning))
      :: ("securityProtocol" -> toJSON(config.securityProtocol.stringId))
      :: Nil)
    compact(render(json)).getBytes(StandardCharsets.UTF_8)
  }

  def deserialize(ba: Array[Byte]) : Try[ClusterConfig] = {
    Try {
      val json = parse(kafka.manager.utils.deserializeString(ba))

      val result = (fieldExtended[String]("name")(json) |@| fieldExtended[CuratorConfig]("curatorConfig")(json) |@| fieldExtended[Boolean]("enabled")(json))
      {
        (name:String,curatorConfig:CuratorConfig,enabled:Boolean) =>
          val versionString = fieldExtended[String]("kafkaVersion")(json)
          val version = versionString.map(KafkaVersion.apply).getOrElse(Kafka_0_8_1_1)
          val jmxEnabled = fieldExtended[Boolean]("jmxEnabled")(json)
          val jmxUser = fieldExtended[Option[String]]("jmxUser")(json)
          val jmxPass = fieldExtended[Option[String]]("jmxPass")(json)
          val jmxSsl = fieldExtended[Boolean]("jmxSsl")(json)
          val pollConsumers = fieldExtended[Boolean]("pollConsumers")(json)
          val filterConsumers = fieldExtended[Boolean]("filterConsumers")(json)
          val logkafkaEnabled = fieldExtended[Boolean]("logkafkaEnabled")(json)
          val activeOffsetCacheEnabled = fieldExtended[Boolean]("activeOffsetCacheEnabled")(json)
          val displaySizeEnabled = fieldExtended[Boolean]("displaySizeEnabled")(json)
          val clusterTuning = fieldExtended[Option[ClusterTuning]]("tuning")(json)
          val securityProtocolString = fieldExtended[String]("securityProtocol")(json)
          val securityProtocol = securityProtocolString.map(SecurityProtocol.apply).getOrElse(PLAINTEXT)

          ClusterConfig.apply(
            name,
            curatorConfig,
            enabled,version,
            jmxEnabled.getOrElse(false),
            jmxUser.getOrElse(None),
            jmxPass.getOrElse(None),
            jmxSsl.getOrElse(false),
            pollConsumers.getOrElse(false),
            filterConsumers.getOrElse(true),
            logkafkaEnabled.getOrElse(false),
            activeOffsetCacheEnabled.getOrElse(false),
            displaySizeEnabled.getOrElse(false),
            clusterTuning.getOrElse(None),
            securityProtocol
          )
      }

      result match {
        case Failure(nel) =>
          throw new IllegalArgumentException(nel.toString())
        case Success(clusterConfig) =>
          clusterConfig
      }

    }
  }

}

case class ClusterTuning(brokerViewUpdatePeriodSeconds: Option[Int]
                         , clusterManagerThreadPoolSize: Option[Int]
                         , clusterManagerThreadPoolQueueSize: Option[Int]
                         , kafkaCommandThreadPoolSize: Option[Int]
                         , kafkaCommandThreadPoolQueueSize: Option[Int]
                         , logkafkaCommandThreadPoolSize: Option[Int]
                         , logkafkaCommandThreadPoolQueueSize: Option[Int]
                         , logkafkaUpdatePeriodSeconds: Option[Int]
                         , partitionOffsetCacheTimeoutSecs: Option[Int]
                         , brokerViewThreadPoolSize: Option[Int]
                         , brokerViewThreadPoolQueueSize: Option[Int]
                         , offsetCacheThreadPoolSize: Option[Int]
                         , offsetCacheThreadPoolQueueSize: Option[Int]
                         , kafkaAdminClientThreadPoolSize: Option[Int]
                         , kafkaAdminClientThreadPoolQueueSize: Option[Int]
                        )
object ClusterTuning {
  import org.json4s._
  import org.json4s.jackson.Serialization
  import org.json4s.scalaz.JsonScalaz._

  import scala.language.reflectiveCalls

  implicit val formats = Serialization.formats(FullTypeHints(List(classOf[ClusterTuning])))

  implicit def clusterTuningJSONW: JSONW[ClusterTuning] = new JSONW[ClusterTuning] {
    def write(tuning: ClusterTuning) =
      makeObj(("brokerViewUpdatePeriodSeconds" -> toJSON(tuning.brokerViewUpdatePeriodSeconds))
        :: ("clusterManagerThreadPoolSize" -> toJSON(tuning.clusterManagerThreadPoolSize))
        :: ("clusterManagerThreadPoolQueueSize" -> toJSON(tuning.clusterManagerThreadPoolQueueSize))
        :: ("kafkaCommandThreadPoolSize" -> toJSON(tuning.kafkaCommandThreadPoolSize))
        :: ("kafkaCommandThreadPoolQueueSize" -> toJSON(tuning.kafkaCommandThreadPoolQueueSize))
        :: ("logkafkaCommandThreadPoolSize" -> toJSON(tuning.logkafkaCommandThreadPoolSize))
        :: ("logkafkaCommandThreadPoolQueueSize" -> toJSON(tuning.logkafkaCommandThreadPoolQueueSize))
        :: ("logkafkaUpdatePeriodSeconds" -> toJSON(tuning.logkafkaUpdatePeriodSeconds))
        :: ("partitionOffsetCacheTimeoutSecs" -> toJSON(tuning.partitionOffsetCacheTimeoutSecs))
        :: ("brokerViewThreadPoolSize" -> toJSON(tuning.brokerViewThreadPoolSize))
        :: ("brokerViewThreadPoolQueueSize" -> toJSON(tuning.brokerViewThreadPoolQueueSize))
        :: ("offsetCacheThreadPoolSize" -> toJSON(tuning.offsetCacheThreadPoolSize))
        :: ("offsetCacheThreadPoolQueueSize" -> toJSON(tuning.offsetCacheThreadPoolQueueSize))
        :: ("kafkaAdminClientThreadPoolSize" -> toJSON(tuning.kafkaAdminClientThreadPoolSize))
        :: ("kafkaAdminClientThreadPoolQueueSize" -> toJSON(tuning.kafkaAdminClientThreadPoolQueueSize))
        :: Nil)
  }

  implicit def clusterTuningJSONR: JSONR[ClusterTuning] = new JSONR[ClusterTuning] {
    def read(json: JValue): Result[ClusterTuning] = {
      for {
        brokerViewUpdatePeriodSeconds <- fieldExtended[Option[Int]]("brokerViewUpdatePeriodSeconds")(json)
        clusterManagerThreadPoolSize <- fieldExtended[Option[Int]]("clusterManagerThreadPoolSize")(json)
        clusterManagerThreadPoolQueueSize <- fieldExtended[Option[Int]]("clusterManagerThreadPoolQueueSize")(json)
        kafkaCommandThreadPoolSize <- fieldExtended[Option[Int]]("kafkaCommandThreadPoolSize")(json)
        kafkaCommandThreadPoolQueueSize <- fieldExtended[Option[Int]]("kafkaCommandThreadPoolQueueSize")(json)
        logkafkaCommandThreadPoolSize <- fieldExtended[Option[Int]]("logkafkaCommandThreadPoolSize")(json)
        logkafkaCommandThreadPoolQueueSize <- fieldExtended[Option[Int]]("logkafkaCommandThreadPoolQueueSize")(json)
        logkafkaUpdatePeriodSeconds <- fieldExtended[Option[Int]]("logkafkaUpdatePeriodSeconds")(json)
        partitionOffsetCacheTimeoutSecs <- fieldExtended[Option[Int]]("partitionOffsetCacheTimeoutSecs")(json)
        brokerViewThreadPoolSize <- fieldExtended[Option[Int]]("brokerViewThreadPoolSize")(json)
        brokerViewThreadPoolQueueSize <- fieldExtended[Option[Int]]("brokerViewThreadPoolQueueSize")(json)
        offsetCacheThreadPoolSize <- fieldExtended[Option[Int]]("offsetCacheThreadPoolSize")(json)
        offsetCacheThreadPoolQueueSize <- fieldExtended[Option[Int]]("offsetCacheThreadPoolQueueSize")(json)
        kafkaAdminClientThreadPoolSize <- fieldExtended[Option[Int]]("kafkaAdminClientThreadPoolSize")(json)
        kafkaAdminClientThreadPoolQueueSize <- fieldExtended[Option[Int]]("kafkaAdminClientThreadPoolQueueSize")(json)
      } yield {
        ClusterTuning(
          brokerViewUpdatePeriodSeconds = brokerViewUpdatePeriodSeconds
          , clusterManagerThreadPoolSize = clusterManagerThreadPoolSize
          , clusterManagerThreadPoolQueueSize = clusterManagerThreadPoolQueueSize
          , kafkaCommandThreadPoolSize = kafkaCommandThreadPoolSize
          , kafkaCommandThreadPoolQueueSize = kafkaCommandThreadPoolQueueSize
          , logkafkaCommandThreadPoolSize = logkafkaCommandThreadPoolSize
          , logkafkaCommandThreadPoolQueueSize = logkafkaCommandThreadPoolQueueSize
          , logkafkaUpdatePeriodSeconds = logkafkaUpdatePeriodSeconds
          , partitionOffsetCacheTimeoutSecs = partitionOffsetCacheTimeoutSecs
          , brokerViewThreadPoolSize = brokerViewThreadPoolSize
          , brokerViewThreadPoolQueueSize = brokerViewThreadPoolQueueSize
          , offsetCacheThreadPoolSize = offsetCacheThreadPoolSize
          , offsetCacheThreadPoolQueueSize = offsetCacheThreadPoolQueueSize
          , kafkaAdminClientThreadPoolSize = kafkaAdminClientThreadPoolSize
          , kafkaAdminClientThreadPoolQueueSize = kafkaAdminClientThreadPoolQueueSize
        )
      }
    }
  }

}

case class ClusterContext(clusterFeatures: ClusterFeatures, config: ClusterConfig)
case class ClusterConfig (name: String
                          , curatorConfig : CuratorConfig
                          , enabled: Boolean
                          , version: KafkaVersion
                          , jmxEnabled: Boolean
                          , jmxUser: Option[String]
                          , jmxPass: Option[String]
                          , jmxSsl: Boolean
                          , pollConsumers: Boolean
                          , filterConsumers: Boolean
                          , logkafkaEnabled: Boolean
                          , activeOffsetCacheEnabled: Boolean
                          , displaySizeEnabled: Boolean
                          , tuning: Option[ClusterTuning]
                          , securityProtocol: SecurityProtocol
                         )

sealed trait SecurityProtocol {
  def stringId: String
  def secure: Boolean
}
case object SASL_PLAINTEXT extends SecurityProtocol {
  val stringId = "SASL_PLAINTEXT"
  val secure = true
}
case object SASL_SSL extends SecurityProtocol {
  val stringId = "SASL_SSL"
  val secure = true
}
case object SSL extends SecurityProtocol {
  val stringId = "SSL"
  val secure = true
}
case object PLAINTEXT extends SecurityProtocol {
  val stringId = "PLAINTEXT"
  val secure = false
}
object SecurityProtocol {
  private[this] val typesMap: Map[String, SecurityProtocol] = Map(
    SASL_PLAINTEXT.stringId -> SASL_PLAINTEXT
    , SASL_SSL.stringId -> SASL_SSL
    , SSL.stringId -> SSL
    , PLAINTEXT.stringId -> PLAINTEXT
  )

  val formSelectList : IndexedSeq[(String,String)] = typesMap.toIndexedSeq.map(t => (t._1,t._2.stringId))
  def apply(s: String) : SecurityProtocol = typesMap(s.toUpperCase)
}
