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

package kafka.manager.utils

import java.util.Properties

import kafka.manager.{Kafka_0_8_2_0, KafkaVersion}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Random

class LogkafkaAdminUtils(version: KafkaVersion) {

  private[this] lazy val logger = LoggerFactory.getLogger(this.getClass)

  val rand = new Random

  def isDeleteSupported : Boolean = {
    version match {
      case Kafka_0_8_2_0 => true
      case _ => false
    }
  }

  def deleteLogkafka(curator: CuratorFramework, 
                   hostname: String, 
                   log_path: String, 
                   logkafkaConfigOption: Option[kafka.manager.ActorModel.LogkafkaConfig]): Unit = {
    logkafkaConfigOption.map { lcg =>
      lcg.config.map { c => 
        val configMap =kafka.manager.utils.Logkafka.parseJsonStr(hostname, c)
        if (!configMap.isEmpty || !(configMap - log_path).isEmpty ) { 
          writeLogkafkaConfig(curator, hostname, configMap - log_path, -1)
        }
      } getOrElse { LogkafkaErrors.HostnameNotExists(hostname) }
    } getOrElse { LogkafkaErrors.HostnameNotExists(hostname) }
  }

  def createLogkafka(curator: CuratorFramework,
                  hostname: String,
                  log_path: String,
                  config: Properties = new Properties,
                  logkafkaConfigOption: Option[kafka.manager.ActorModel.LogkafkaConfig]
                  ): Unit = {
    createOrUpdateLogkafkaConfigPathInZK(curator, hostname, log_path, config, logkafkaConfigOption)
  }

  def createOrUpdateLogkafkaConfigPathInZK(curator: CuratorFramework,
                                           hostname: String,
                                           log_path: String,
                                           config: Properties = new Properties,
                                           logkafkaConfigOption: Option[kafka.manager.ActorModel.LogkafkaConfig],
                                           update: Boolean = false,
                                           readVersion: Int = -1
                                           ) {
    // validate arguments
    Logkafka.validateHostname(hostname)
    LogkafkaNewConfigs.validate(version,config)

    val configMap: mutable.Map[String, String] = {
      import scala.collection.JavaConverters._
      config.asScala
    }
    val newConfigMap = Map(log_path -> Map(configMap.toSeq:_*))

    val logkafkaConfigMap = logkafkaConfigOption.map { lcg =>
      lcg.config.map { c =>
        kafka.manager.utils.Logkafka.parseJsonStr(hostname, c)
      } getOrElse { Map.empty }
    } getOrElse { Map.empty }

    if(!update ) {
      // write out the config on create, not update, if there is any
      writeLogkafkaConfig(curator, hostname, logkafkaConfigMap ++ newConfigMap, readVersion)
    } else {
      val merged = logkafkaConfigMap.toSeq ++ newConfigMap.toSeq
      val grouped = merged.groupBy(_._1)
      val cleaned = grouped.mapValues(_.map(_._2).fold(Map.empty)(_ ++ _))
      writeLogkafkaConfig(curator, hostname, cleaned, readVersion)
    }
  }

  /**
   * Update the config for an existing (hostname,log_path)
   * @param curator: The zk client handle used to write the new config to zookeeper
   * @param hostname: The hostname for which configs are being changed
   * @param log_path: The log_path for which configs are being changed
   * @param config: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
  def changeLogkafkaConfig(curator: CuratorFramework,
                  hostname: String,
                  log_path: String,
                  config: Properties = new Properties,
                  logkafkaConfigOption: Option[kafka.manager.ActorModel.LogkafkaConfig]
                  ): Unit = {
    createOrUpdateLogkafkaConfigPathInZK(curator, hostname, log_path, config, logkafkaConfigOption, true)
  }

  /**
   * Write out the logkafka config to zk, if there is any
   */
  private def writeLogkafkaConfig(curator: CuratorFramework, hostname: String, configMap: Map[String, Map[String, String]], readVersion: Int = -1) {
    ZkUtils.updatePersistentPath(curator, LogkafkaZkUtils.getLogkafkaConfigPath(hostname), toJson(configMap), readVersion)
  }
}
