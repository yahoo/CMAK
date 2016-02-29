/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.model

import java.nio.charset.StandardCharsets

import kafka.manager.features.ClusterFeatures

import scala.util.Try
import scala.util.matching.Regex

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

object KafkaVersion {
  val supportedVersions: Map[String,KafkaVersion] = Map(
    "0.8.1.1" -> Kafka_0_8_1_1,
    "0.8.2-beta" -> Kafka_0_8_2_0,
    "0.8.2.0" -> Kafka_0_8_2_0,
    "0.8.2.1" -> Kafka_0_8_2_1,
    "0.8.2.2" -> Kafka_0_8_2_2,
    "0.9.0.0" -> Kafka_0_9_0_0,
    "0.9.0.1" -> Kafka_0_9_0_1
  )

  val formSelectList : IndexedSeq[(String,String)] = supportedVersions.toIndexedSeq.filterNot(_._1.contains("beta")).map(t => (t._1,t._2.toString))

  def apply(s: String) : KafkaVersion = {
    supportedVersions.get(s) match {
      case Some(v) => v
      case None => throw new IllegalArgumentException(s"Unsupported kafka version : $s")
    }
  }

  def unapply(v: KafkaVersion) : Option[String] = {
    Some(v.toString)
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

  def apply(name: String,
            version : String,
            zkHosts: String,
            zkMaxRetry: Int = 100,
            jmxEnabled: Boolean,
            jmxUser: Option[String],
            jmxPass: Option[String],
            pollConsumers: Boolean,
            filterConsumers: Boolean,
            logkafkaEnabled: Boolean = false,
            activeOffsetCacheEnabled: Boolean = false,
            displaySizeEnabled: Boolean = false) : ClusterConfig = {
    val kafkaVersion = KafkaVersion(version)
    //validate cluster name
    validateName(name)
    //validate zk hosts
    validateZkHosts(zkHosts)
    val cleanZkHosts = zkHosts.replaceAll(" ","")
    new ClusterConfig(
      name,
      CuratorConfig(cleanZkHosts, zkMaxRetry),
      true,
      kafkaVersion,
      jmxEnabled,
      jmxUser,
      jmxPass,
      pollConsumers,
      filterConsumers,
      logkafkaEnabled,
      activeOffsetCacheEnabled,
      displaySizeEnabled)
  }

  def customUnapply(cc: ClusterConfig) : Option[(
    String, String, String, Int, Boolean, Option[String], Option[String],
      Boolean, Boolean, Boolean, Boolean, Boolean)] = {
    Some((cc.name, cc.version.toString, cc.curatorConfig.zkConnect, cc.curatorConfig.zkMaxRetry,
      cc.jmxEnabled, cc.jmxUser, cc.jmxPass, cc.pollConsumers, cc.filterConsumers,
      cc.logkafkaEnabled, cc.activeOffsetCacheEnabled, cc.displaySizeEnabled))
  }

  import scalaz.{Failure,Success}
  import scalaz.syntax.applicative._
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
    field[String]("zkConnect"), field[Int]("zkMaxRetry"), field[Int]("baseSleepTimeMs"), field[Int]("maxSleepTimeMs"))

  def serialize(config: ClusterConfig) : Array[Byte] = {
    val json = makeObj(("name" -> toJSON(config.name))
      :: ("curatorConfig" -> toJSON(config.curatorConfig))
      :: ("enabled" -> toJSON(config.enabled))
      :: ("kafkaVersion" -> toJSON(config.version.toString))
      :: ("jmxEnabled" -> toJSON(config.jmxEnabled))
      :: ("jmxUser" -> toJSON(config.jmxUser))
      :: ("jmxPass" -> toJSON(config.jmxPass))
      :: ("pollConsumers" -> toJSON(config.pollConsumers))
      :: ("filterConsumers" -> toJSON(config.filterConsumers))
      :: ("logkafkaEnabled" -> toJSON(config.logkafkaEnabled))
      :: ("activeOffsetCacheEnabled" -> toJSON(config.activeOffsetCacheEnabled))
      :: ("displaySizeEnabled" -> toJSON(config.displaySizeEnabled))
      :: Nil)
    compact(render(json)).getBytes(StandardCharsets.UTF_8)
  }

  def deserialize(ba: Array[Byte]) : Try[ClusterConfig] = {
    Try {
      val json = parse(kafka.manager.utils.deserializeString(ba))

      val result = (field[String]("name")(json) |@| field[CuratorConfig]("curatorConfig")(json) |@| field[Boolean]("enabled")(json))
      {
        (name:String,curatorConfig:CuratorConfig,enabled:Boolean) =>
          val versionString = field[String]("kafkaVersion")(json)
          val version = versionString.map(KafkaVersion.apply).getOrElse(Kafka_0_8_1_1)
          val jmxEnabled = field[Boolean]("jmxEnabled")(json)
          val jmxUser = field[Option[String]]("jmxUser")(json)
          val jmxPass = field[Option[String]]("jmxPass")(json)
          val pollConsumers = field[Boolean]("pollConsumers")(json)
          val filterConsumers = field[Boolean]("filterConsumers")(json)
          val logkafkaEnabled = field[Boolean]("logkafkaEnabled")(json)
          val activeOffsetCacheEnabled = field[Boolean]("activeOffsetCacheEnabled")(json)
          val displaySizeEnabled = field[Boolean]("displaySizeEnabled")(json)
          ClusterConfig.apply(
            name,
            curatorConfig,
            enabled,version,
            jmxEnabled.getOrElse(false),
            jmxUser.getOrElse(None),
            jmxPass.getOrElse(None),
            pollConsumers.getOrElse(false),
            filterConsumers.getOrElse(true),
            logkafkaEnabled.getOrElse(false),
            activeOffsetCacheEnabled.getOrElse(false),
            displaySizeEnabled.getOrElse(false)
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

case class ClusterContext(clusterFeatures: ClusterFeatures, config: ClusterConfig)
case class ClusterConfig (name: String,
                          curatorConfig : CuratorConfig,
                          enabled: Boolean,
                          version: KafkaVersion,
                          jmxEnabled: Boolean,
                          jmxUser: Option[String],
                          jmxPass: Option[String],
                          pollConsumers: Boolean,
                          filterConsumers: Boolean,
                          logkafkaEnabled: Boolean,
                          activeOffsetCacheEnabled: Boolean,
                          displaySizeEnabled: Boolean)
