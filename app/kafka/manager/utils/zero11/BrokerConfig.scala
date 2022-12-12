/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.manager.utils.zero11

import java.util.Properties

import kafka.manager.utils.BrokerConfigs
import kafka.server.ReplicationQuotaManagerConfig
import org.apache.kafka.common.config.ConfigDef.{ConfigKey, Validator}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.errors.InvalidConfigurationException

import scala.collection.JavaConverters._
import scala.collection.mutable



case class BrokerConfig(props: java.util.Map[_, _]) extends AbstractConfig(BrokerConfig.configDef, props, false) {

}

object BrokerConfig extends BrokerConfigs {

  //Properties
  val LeaderReplicationThrottledRateProp = "leader.replication.throttled.rate"
  val FollowerReplicationThrottledRateProp = "follower.replication.throttled.rate"

  //Defaults
  val DefaultReplicationThrottledRate = ReplicationQuotaManagerConfig.QuotaBytesPerSecondDefault

  //Documentation
  val LeaderReplicationThrottledRateDoc = "A long representing the upper bound (bytes/sec) on replication traffic for leaders enumerated in the " +
    s"property ${LogConfig.LeaderReplicationThrottledReplicasProp} (for each topic). This property can be only set dynamically. It is suggested that the " +
    s"limit be kept above 1MB/s for accurate behaviour."
  val FollowerReplicationThrottledRateDoc = "A long representing the upper bound (bytes/sec) on replication traffic for followers enumerated in the " +
    s"property ${LogConfig.FollowerReplicationThrottledReplicasProp} (for each topic). This property can be only set dynamically. It is suggested that the " +
    s"limit be kept above 1MB/s for accurate behaviour."


  private class BrokerConfigDef extends ConfigDef {
    private final val serverDefaultConfigNames = mutable.Map[String, String]()

    def define(name: String, defType: ConfigDef.Type, defaultValue: Any, validator: Validator,
               importance: ConfigDef.Importance, doc: String, serverDefaultConfigName: String): BrokerConfigDef = {
      super.define(name, defType, defaultValue, validator, importance, doc)
      serverDefaultConfigNames.put(name, serverDefaultConfigName)
      this
    }

    def define(name: String, defType: ConfigDef.Type, defaultValue: Any, importance: ConfigDef.Importance,
               documentation: String, serverDefaultConfigName: String): BrokerConfigDef = {
      super.define(name, defType, defaultValue, importance, documentation)
      serverDefaultConfigNames.put(name, serverDefaultConfigName)
      this
    }

    def define(name: String, defType: ConfigDef.Type, importance: ConfigDef.Importance, documentation: String,
               serverDefaultConfigName: String): BrokerConfigDef = {
      super.define(name, defType, importance, documentation)
      serverDefaultConfigNames.put(name, serverDefaultConfigName)
      this
    }

    override def headers = List("Name", "Description", "Type", "Default", "Valid Values", "Server Default Property", "Importance").asJava

    override def getConfigValue(key: ConfigKey, headerName: String): String = {
      headerName match {
        case "Server Default Property" => serverDefaultConfigNames.get(key.name).get
        case _ => super.getConfigValue(key, headerName)
      }
    }

    def serverConfigName(configName: String): Option[String] = serverDefaultConfigNames.get(configName)
  }
  private val configDef: BrokerConfigDef = {
    import org.apache.kafka.common.config.ConfigDef.Importance._
    import org.apache.kafka.common.config.ConfigDef.Range._
    import org.apache.kafka.common.config.ConfigDef.Type._
    new BrokerConfigDef()
      .define(LeaderReplicationThrottledRateProp, LONG, DefaultReplicationThrottledRate, atLeast(0), MEDIUM, LeaderReplicationThrottledRateDoc
      ,LeaderReplicationThrottledRateProp)
      .define(FollowerReplicationThrottledRateProp, LONG, DefaultReplicationThrottledRate, atLeast(0), MEDIUM, FollowerReplicationThrottledRateDoc
      ,LeaderReplicationThrottledRateProp)
  }

  def configNames: Seq[String] = configDef.names.asScala.toSeq.sorted
  /**
   * Check that property names are valid
   */
  def validateNames(props: Properties) {
    val names = configNames
    for(name <- props.asScala.keys)
      if (!names.contains(name))
        throw new InvalidConfigurationException(s"Unknown Log configuration $name.")
  }
  /**
   * Check that the given properties contain only valid log config names and that all values can be parsed and are valid
   */
  def validate(props: Properties) {
    validateNames(props)
    configDef.parse(props)
  }

  def configNamesAndDoc: Seq[(String, String)] = {
    Option(configDef).fold {
      configNames.map(n => n -> "")
    } {
      configDef =>
        val keyMap = configDef.configKeys()
        configNames.map(n => n -> Option(keyMap.get(n)).map(_.documentation).flatMap(Option.apply).getOrElse(""))
    }
  }
}
