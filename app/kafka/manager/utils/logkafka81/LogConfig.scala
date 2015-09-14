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

package kafka.manager.utils.logkafka81

import java.util.Properties
import kafka.manager.utils.LogkafkaNewConfigs

object Defaults {
  val Valid = true
  val FollowLast = true
  val BatchSize = 200
  val Topic = ""
  val Key = ""
  val Partition = -1
  val CompressionCodec= "none"
  val RequiredAcks = 1
  val MessageTimeoutMs = 0
}

/**
 * Configuration settings for a log
 * @param valid Enable now or not
 * @param followLast If set to "false", when restarting logkafka process, the log_path formatted with current time will be collect; 
                     If set to "true", when restarting logkafka process, the last collecting file will be collected continually
 * @param batchSize The batch size of messages to be sent 
 * @param topic The topic of messages to be sent
 * @param key The key of messages to be sent
 * @param partition The partition of messages to be sent. 
                    -1 : random 
                    n(>=0): partition n
 * @param compressionCodec Optional compression method of messages: none, gzip, snappy
 * @param requiredAcks Number of required acks
 * @param messageTimeoutMs Local message timeout. This value is only enforced locally 
                           and limits the time a produced message waits for successful delivery.
                           A time of 0 is infinite.
 *
 */
case class LogConfig(val valid: Boolean = Defaults.Valid,
                     val followLast: Boolean = Defaults.FollowLast,
                     val batchSize: Long = Defaults.BatchSize,
                     val topic: String = Defaults.Topic,
                     val key: String = Defaults.Key,
                     val partition: Int = Defaults.Partition,
                     val compressionCodec: String = Defaults.CompressionCodec,
                     val requiredAcks: Int = Defaults.RequiredAcks,
                     val messageTimeoutMs: Long = Defaults.MessageTimeoutMs) {

  def toProps: Properties = {
    val props = new Properties()
    import LogConfig._
    props.put(ValidProp, valid.toString)
    props.put(FollowLastProp, followLast.toString)
    props.put(BatchSizeProp, batchSize.toString)
    props.put(TopicProp, topic.toString)
    props.put(KeyProp, key.toString)
    props.put(PartitionProp, partition.toString)
    props.put(CompressionCodecProp, compressionCodec.toString)
    props.put(RequiredAcksProp, requiredAcks.toString)
    props.put(MessageTimeoutMsProp, messageTimeoutMs.toString)
    props
  }

  /**
   * Get the absolute value of the given number. If the number is Int.MinValue return 0.
   * This is different from java.lang.Math.abs or scala.math.abs in that they return Int.MinValue (!).
   */
  def abs(n: Int) = if(n == Integer.MIN_VALUE) 0 else math.abs(n)
}

object LogConfig extends LogkafkaNewConfigs {
  val ValidProp = "valid"
  val FollowLastProp = "follow_last"
  val BatchSizeProp = "batchsize"
  val TopicProp = "topic"
  val KeyProp = "key"
  val PartitionProp = "partition"
  val CompressionCodecProp  = "compression_codec"
  val RequiredAcksProp = "required_acks"
  val MessageTimeoutMsProp = "message_timeout_ms"

  val ConfigMaps = Map(ValidProp -> Defaults.Valid.toString,
                       FollowLastProp -> Defaults.FollowLast.toString,
                       BatchSizeProp -> Defaults.BatchSize.toString,
                       TopicProp -> Defaults.Topic.toString,
                       KeyProp -> Defaults.Key.toString,
                       PartitionProp -> Defaults.Partition.toString,
                       CompressionCodecProp -> Defaults.CompressionCodec.toString,
                       RequiredAcksProp -> Defaults.RequiredAcks.toString,
                       MessageTimeoutMsProp -> Defaults.MessageTimeoutMs.toString)
  def configMaps = ConfigMaps
  val ConfigNames = ConfigMaps.keySet
  def configNames = ConfigNames
  
  /**
   * Parse the given properties instance into a LogConfig object
   */
  def fromProps(props: Properties): LogConfig = {
    new LogConfig(valid = props.getProperty(ValidProp, Defaults.Valid.toString).toBoolean,
                  followLast = props.getProperty(FollowLastProp, Defaults.FollowLast.toString).toBoolean,
                  batchSize = props.getProperty(BatchSizeProp, Defaults.BatchSize.toString).toLong,
                  topic = props.getProperty(TopicProp, Defaults.Topic.toString).toString,
                  key = props.getProperty(KeyProp, Defaults.Key.toString).toString,
                  partition = props.getProperty(PartitionProp, Defaults.Partition.toString).toInt,
                  compressionCodec = props.getProperty(CompressionCodecProp, Defaults.CompressionCodec.toString).toString,
                  requiredAcks= props.getProperty(RequiredAcksProp, Defaults.RequiredAcks.toString).toInt,
                  messageTimeoutMs = props.getProperty(MessageTimeoutMsProp, Defaults.MessageTimeoutMs.toString).toLong)
  }

  /**
   * Create a log config instance using the given properties and defaults
   */
  def fromProps(defaults: Properties, overrides: Properties): LogConfig = {
    val props = new Properties(defaults)
    props.putAll(overrides)
    fromProps(props)
  }

  /**
   * Check that property names are valid
   */
  def validateNames(props: Properties) {
    import scala.collection.JavaConverters._
    for(name <- props.keys().asScala)
      require(LogConfig.ConfigNames.asJava.contains(name), "Unknown configuration \"%s\".".format(name))
  }

  /**
   * Check that the given properties contain only valid log config names, and that all values can be parsed.
   */
  def validate(props: Properties) {
    validateNames(props)
    validateTopic(props)
    LogConfig.fromProps(LogConfig().toProps, props) // check that we can parse the values
  }

  /**
   * Check that Topic is reasonable
   */
  private def validateTopic(props: Properties) {
    val topic = props.getProperty(TopicProp)
    require(topic != null , "Topic is null")
  }

}
