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

package kafka.manager.utils.logkafka82

import kafka.manager._
import kafka.manager.utils.LogkafkaNewConfigs

import java.util.Properties

object Defaults {
  val Valid = true
  val FollowLast = true
  val ReadFromHead = true
  val BatchSize = 200
  val LineDelimiter = 10 // 10 means ascii '\n'
  val RemoveDelimiter = true
  val Topic = ""
  val Key = ""
  val Partition = -1
  val CompressionCodec= "none"
  val RequiredAcks = 1
  val MessageTimeoutMs = 0
  val RegexFilterPattern = ""
  val LaggingMaxBytes = 0
  val RotateLaggingMaxSec = 0
}

/**
 * Configuration settings for a log
 * @param valid Enable now or not
 * @param followLast If set to "false", when restarting logkafka process, the log_path formatted with current time will be collect; 
                     If set to "true", when restarting logkafka process, the last collecting file will be collected continually
 * @param readFromHead If set to "false", the first file will be collected from tail;
                     If set to "true", the first file will be collected from head
 * @param batchSize The batch size of messages to be sent 
 * @param lineDelimiter Delimiter of log file lines
 * @param removeDelimiter Remove delimiter or not when collecting log file lines
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
 * @param regexFilterPattern The messages matching this pattern will be dropped.
 *
 */
case class LogConfig(val valid: Boolean = Defaults.Valid,
                     val followLast: Boolean = Defaults.FollowLast,
                     val readFromHead: Boolean = Defaults.ReadFromHead,
                     val batchSize: Long = Defaults.BatchSize,
                     val lineDelimiter: Int = Defaults.LineDelimiter,
                     val removeDelimiter: Boolean = Defaults.RemoveDelimiter,
                     val topic: String = Defaults.Topic,
                     val key: String = Defaults.Key,
                     val partition: Int = Defaults.Partition,
                     val compressionCodec: String = Defaults.CompressionCodec,
                     val requiredAcks: Int = Defaults.RequiredAcks,
                     val messageTimeoutMs: Long = Defaults.MessageTimeoutMs,
                     val regexFilterPattern: String = Defaults.RegexFilterPattern,
                     val laggingMaxBytes: Long = Defaults.LaggingMaxBytes,
                     val rotateLaggingMaxSec: Long = Defaults.RotateLaggingMaxSec) {

  def toProps: Properties = {
    val props = new Properties()
    import LogConfig._
    props.put(ValidProp, valid.toString)
    props.put(FollowLastProp, followLast.toString)
    props.put(ReadFromHeadProp, readFromHead.toString)
    props.put(BatchSizeProp, batchSize.toString)
    props.put(LineDelimiterProp, lineDelimiter.toString)
    props.put(RemoveDelimiterProp, removeDelimiter.toString)
    props.put(TopicProp, topic.toString)
    props.put(KeyProp, key.toString)
    props.put(PartitionProp, partition.toString)
    props.put(CompressionCodecProp, compressionCodec.toString)
    props.put(RequiredAcksProp, requiredAcks.toString)
    props.put(MessageTimeoutMsProp, messageTimeoutMs.toString)
    props.put(RegexFilterPatternProp, regexFilterPattern.toString)
    props
  }

  /**
   * Get the absolute value of the given number. If the number is Int.MinValue return 0.
   * This is different from java.lang.Math.abs or scala.math.abs in that they return Int.MinValue (!).
   */
  def abs(n: Int) = if(n == Integer.MIN_VALUE) 0 else math.abs(n)
}

object LogConfig extends LogkafkaNewConfigs {
  import kafka.manager.utils._

  val minLineDelimiter = 0
  val maxLineDelimiter = 255
  val maxRegexFilterPatternLength = 255

  val ValidProp = "valid"
  val FollowLastProp = "follow_last"
  val ReadFromHeadProp = "read_from_head"
  val BatchSizeProp = "batchsize"
  val LineDelimiterProp = "line_delimiter"
  val RemoveDelimiterProp = "remove_delimiter"
  val TopicProp = "topic"
  val KeyProp = "key"
  val PartitionProp = "partition"
  val CompressionCodecProp  = "compression_codec"
  val RequiredAcksProp = "required_acks"
  val MessageTimeoutMsProp = "message_timeout_ms"
  val RegexFilterPatternProp = "regex_filter_pattern"
  val LaggingMaxBytesProp = "lagging_max_bytes"
  val RotateLaggingMaxSecProp = "rotate_lagging_max_sec"

  val ConfigMaps = Map(ValidProp -> Defaults.Valid.toString,
                       FollowLastProp -> Defaults.FollowLast.toString,
                       ReadFromHeadProp -> Defaults.ReadFromHead.toString,
                       BatchSizeProp -> Defaults.BatchSize.toString,
                       LineDelimiterProp -> Defaults.LineDelimiter.toString,
                       RemoveDelimiterProp -> Defaults.RemoveDelimiter.toString,
                       TopicProp -> Defaults.Topic.toString,
                       KeyProp -> Defaults.Key.toString,
                       PartitionProp -> Defaults.Partition.toString,
                       CompressionCodecProp -> Defaults.CompressionCodec.toString,
                       RequiredAcksProp -> Defaults.RequiredAcks.toString,
                       MessageTimeoutMsProp -> Defaults.MessageTimeoutMs.toString,
                       RegexFilterPatternProp -> Defaults.RegexFilterPattern.toString,
                       LaggingMaxBytesProp -> Defaults.LaggingMaxBytes.toString,
                       RotateLaggingMaxSecProp -> Defaults.RotateLaggingMaxSec.toString)
  def configMaps = ConfigMaps
  val ConfigNames = ConfigMaps.keySet
  def configNames = ConfigNames
  
  /**
   * Parse the given properties instance into a LogConfig object
   */
  def fromProps(props: Properties): LogConfig = {
    new LogConfig(valid = props.getProperty(ValidProp, Defaults.Valid.toString).toBoolean,
                  followLast = props.getProperty(FollowLastProp, Defaults.FollowLast.toString).toBoolean,
                  readFromHead = props.getProperty(ReadFromHeadProp, Defaults.ReadFromHead.toString).toBoolean,
                  batchSize = props.getProperty(BatchSizeProp, Defaults.BatchSize.toString).toLong,
                  lineDelimiter = props.getProperty(LineDelimiterProp, Defaults.LineDelimiter.toString).toInt,
                  removeDelimiter = props.getProperty(RemoveDelimiterProp, Defaults.RemoveDelimiter.toString).toBoolean,
                  topic = props.getProperty(TopicProp, Defaults.Topic.toString).toString,
                  key = props.getProperty(KeyProp, Defaults.Key.toString).toString,
                  partition = props.getProperty(PartitionProp, Defaults.Partition.toString).toInt,
                  compressionCodec = props.getProperty(CompressionCodecProp, Defaults.CompressionCodec.toString).toString,
                  requiredAcks= props.getProperty(RequiredAcksProp, Defaults.RequiredAcks.toString).toInt,
                  messageTimeoutMs = props.getProperty(MessageTimeoutMsProp, Defaults.MessageTimeoutMs.toString).toLong,
                  regexFilterPattern = props.getProperty(RegexFilterPatternProp, Defaults.RegexFilterPattern.toString).toString,
                  laggingMaxBytes = props.getProperty(LaggingMaxBytesProp, Defaults.LaggingMaxBytes.toString).toLong,
                  rotateLaggingMaxSec = props.getProperty(RotateLaggingMaxSecProp, Defaults.RotateLaggingMaxSec.toString).toLong)
  }

  /**
   * Create a log config instance using the given properties and defaults
   */
  def fromProps(defaults: Properties, overrides: Properties): LogConfig = {
    val props = new Properties(defaults)
    props.putAll(overrides.asMap)
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
    validateLineDelimiter(props)
    validateTopic(props)
    validateRegexFilterPattern(props)
    LogConfig.fromProps(LogConfig().toProps, props) // check that we can parse the values
  }

  /**
   * Check that LineDelimiter is reasonable
   */
  private def validateLineDelimiter(props: Properties) {
    val lineDelimiter = props.getProperty(LineDelimiterProp)
    if (lineDelimiter == null) return
    checkCondition(lineDelimiter.toInt >= minLineDelimiter && lineDelimiter.toInt <= maxLineDelimiter, LogkafkaConfigErrors.InvalidLineDelimiter)
  }

  /**
   * Check that Topic is reasonable
   */
  private def validateTopic(props: Properties) {
    val topic = props.getProperty(TopicProp)
    require(topic != null , "Topic is null")
  }

  /**
   * Check that is RegexFilterPattern reasonable
   */
  private def validateRegexFilterPattern(props: Properties) {
    val regexFilterPattern = props.getProperty(RegexFilterPatternProp)
    if (regexFilterPattern == null) return
    checkCondition(regexFilterPattern.length <= maxRegexFilterPatternLength, LogkafkaConfigErrors.InvalidRegexFilterPatternLength)
    val valid = try {
      s"""$regexFilterPattern""".r  
      true
    } catch {
      case e: Exception => false
    }
    checkCondition(valid, LogkafkaConfigErrors. InvalidRegexFilterPattern)
  }
}

object LogkafkaConfigErrors {
  import kafka.manager.utils.UtilError
  class InvalidLineDelimiter private[LogkafkaConfigErrors] extends UtilError(
    "line delimiter is illegal, should be an decimal number between 0 and 255")
  class InvalidRegexFilterPattern private[LogkafkaConfigErrors] extends UtilError(
    "regex filter pattern is illegal, does not conform to pcre2")
  class InvalidRegexFilterPatternLength private[LogkafkaConfigErrors] extends UtilError(
    "regex filter pattern is illegal, can't be longer than " + LogConfig.maxRegexFilterPatternLength + " characters")

  val InvalidLineDelimiter = new InvalidLineDelimiter
  val InvalidRegexFilterPattern = new InvalidRegexFilterPattern
  val InvalidRegexFilterPatternLength = new InvalidRegexFilterPatternLength 
}
