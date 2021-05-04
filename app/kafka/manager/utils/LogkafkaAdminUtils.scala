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

import grizzled.slf4j.Logging
import kafka.manager.model.{ActorModel, KafkaVersion, Kafka_0_8_2_0}
import org.apache.curator.framework.CuratorFramework

import java.util.Properties
import scala.collection.mutable
import scala.util.Random

class LogkafkaAdminUtils(version: KafkaVersion) extends Logging {

  val rand = new Random

  def isDeleteSupported : Boolean = {
    version match {
      case Kafka_0_8_2_0 => true
      case _ => false
    }
  }

  def deleteLogkafka(curator: CuratorFramework, 
                   logkafka_id: String, 
                   log_path: String, 
                   logkafkaConfigOption: Option[ActorModel.LogkafkaConfig]): Unit = {
    logkafkaConfigOption.map { lcg =>
      lcg.config.map { c => 
        val configMap =kafka.manager.utils.Logkafka.parseJsonStr(logkafka_id, c)
        if (!configMap.isEmpty || !(configMap - log_path).isEmpty ) { 
          writeLogkafkaConfig(curator, logkafka_id, configMap - log_path, -1)
        }
      } getOrElse { LogkafkaErrors.LogkafkaIdNotExists(logkafka_id) }
    } getOrElse { LogkafkaErrors.LogkafkaIdNotExists(logkafka_id) }
  }

  def createLogkafka(curator: CuratorFramework,
                  logkafka_id: String,
                  log_path: String,
                  config: Properties = new Properties,
                  logkafkaConfigOption: Option[ActorModel.LogkafkaConfig] = None
                  ): Unit = {
    createOrUpdateLogkafkaConfigPathInZK(curator, logkafka_id, log_path, config, logkafkaConfigOption)
  }

  def createOrUpdateLogkafkaConfigPathInZK(curator: CuratorFramework,
                                           logkafka_id: String,
                                           log_path: String,
                                           config: Properties = new Properties,
                                           logkafkaConfigOption: Option[ActorModel.LogkafkaConfig],
                                           update: Boolean = false,
                                           readVersion: Int = -1,
                                           checkConfig: Boolean = true 
                                           ) {
    // validate arguments
    Logkafka.validateLogkafkaId(logkafka_id)
    Logkafka.validatePath(log_path)

    if (checkConfig) {
      LogkafkaNewConfigs.validate(version, config)
    }

    val configMap: mutable.Map[String, String] = {
      import scala.collection.JavaConverters._
      config.asScala
    }
    val newConfigMap = Map(log_path -> Map(configMap.toSeq:_*))

    val logkafkaConfigMap = logkafkaConfigOption.map { lcg =>
      lcg.config.map { c =>
        kafka.manager.utils.Logkafka.parseJsonStr(logkafka_id, c)
      } getOrElse { Map.empty }
    } getOrElse { Map.empty }

    if(!update ) {
      // write out the config on create, not update, if there is any
      writeLogkafkaConfig(curator, logkafka_id, logkafkaConfigMap ++ newConfigMap, readVersion)
    } else {
      val merged = logkafkaConfigMap.toSeq ++ newConfigMap.toSeq
      val grouped = merged.groupBy(_._1)
      val cleaned = grouped.mapValues(_.map(_._2).fold(Map.empty)(_ ++ _))
      writeLogkafkaConfig(curator, logkafka_id, cleaned, readVersion)
    }
  }

  /**
   * Update the config for an existing (logkafka_id,log_path)
   * @param curator: The zk client handle used to write the new config to zookeeper
   * @param logkafka_id: The logkafka_id for which configs are being changed
   * @param log_path: The log_path for which configs are being changed
   * @param config: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
  def changeLogkafkaConfig(curator: CuratorFramework,
                  logkafka_id: String,
                  log_path: String,
                  config: Properties = new Properties,
                  logkafkaConfigOption: Option[ActorModel.LogkafkaConfig],
                  checkConfig: Boolean = true
                  ): Unit = {
    createOrUpdateLogkafkaConfigPathInZK(curator, logkafka_id, log_path, config, logkafkaConfigOption, true, -1, checkConfig)
  }

  /**
   * Write out the logkafka config to zk, if there is any
   */
  private def writeLogkafkaConfig(curator: CuratorFramework, logkafka_id: String, configMap: Map[String, Map[String, String]], readVersion: Int = -1) {
    ZkUtils.updatePersistentPath(curator, LogkafkaZkUtils.getLogkafkaConfigPath(logkafka_id), toJson(configMap), readVersion)
  }
}
