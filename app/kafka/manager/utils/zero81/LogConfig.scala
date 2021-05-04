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

package kafka.manager.utils.zero81

import kafka.manager._
import kafka.manager.utils.TopicConfigs

import java.util.Properties

/**
 * Copied from kafka 0.8.1.1
 * https://git-wip-us.apache.org/repos/asf?p=kafka.git;a=blob;f=core/src/main/scala/kafka/log/LogConfig.scala
 */
case class LogConfig(segmentSize: Int = 1024*1024,
                     segmentMs: Long = Long.MaxValue,
                     flushInterval: Long = Long.MaxValue,
                     flushMs: Long = Long.MaxValue,
                     retentionSize: Long = Long.MaxValue,
                     retentionMs: Long = Long.MaxValue,
                     maxMessageSize: Int = Int.MaxValue,
                     maxIndexSize: Int = 1024*1024,
                     indexInterval: Int = 4096,
                     fileDeleteDelayMs: Long = 60*1000,
                     deleteRetentionMs: Long = 24 * 60 * 60 * 1000L,
                     minCleanableRatio: Double = 0.5,
                     compact: Boolean = false) {

  def toProps: Properties = {
    val props = new Properties()
    import kafka.manager.utils.zero81.LogConfig._
    props.put(SegmentBytesProp, segmentSize.toString)
    props.put(SegmentMsProp, segmentMs.toString)
    props.put(SegmentIndexBytesProp, maxIndexSize.toString)
    props.put(FlushMessagesProp, flushInterval.toString)
    props.put(FlushMsProp, flushMs.toString)
    props.put(RetentionBytesProp, retentionSize.toString)
    props.put(RententionMsProp, retentionMs.toString)
    props.put(MaxMessageBytesProp, maxMessageSize.toString)
    props.put(IndexIntervalBytesProp, indexInterval.toString)
    props.put(DeleteRetentionMsProp, deleteRetentionMs.toString)
    props.put(FileDeleteDelayMsProp, fileDeleteDelayMs.toString)
    props.put(MinCleanableDirtyRatioProp, minCleanableRatio.toString)
    props.put(CleanupPolicyProp, if (compact) "compact" else "delete")
    props
  }
}

object LogConfig extends TopicConfigs {
  val SegmentBytesProp = "segment.bytes"
  val SegmentMsProp = "segment.ms"
  val SegmentIndexBytesProp = "segment.index.bytes"
  val FlushMessagesProp = "flush.messages"
  val FlushMsProp = "flush.ms"
  val RetentionBytesProp = "retention.bytes"
  val RententionMsProp = "retention.ms"
  val MaxMessageBytesProp = "max.message.bytes"
  val IndexIntervalBytesProp = "index.interval.bytes"
  val DeleteRetentionMsProp = "delete.retention.ms"
  val FileDeleteDelayMsProp = "file.delete.delay.ms"
  val MinCleanableDirtyRatioProp = "min.cleanable.dirty.ratio"
  val CleanupPolicyProp = "cleanup.policy"

  val ConfigNames = Seq(SegmentBytesProp,
    SegmentMsProp,
    SegmentIndexBytesProp,
    FlushMessagesProp,
    FlushMsProp,
    RetentionBytesProp,
    RententionMsProp,
    MaxMessageBytesProp,
    IndexIntervalBytesProp,
    FileDeleteDelayMsProp,
    DeleteRetentionMsProp,
    MinCleanableDirtyRatioProp,
    CleanupPolicyProp)
  
  def configNames = ConfigNames

  /**
   * Parse the given properties instance into a LogConfig object
   */
  def fromProps(props: Properties): LogConfig = {
    new LogConfig(segmentSize = props.getProperty(SegmentBytesProp).toInt,
      segmentMs = props.getProperty(SegmentMsProp).toLong,
      maxIndexSize = props.getProperty(SegmentIndexBytesProp).toInt,
      flushInterval = props.getProperty(FlushMessagesProp).toLong,
      flushMs = props.getProperty(FlushMsProp).toLong,
      retentionSize = props.getProperty(RetentionBytesProp).toLong,
      retentionMs = props.getProperty(RententionMsProp).toLong,
      maxMessageSize = props.getProperty(MaxMessageBytesProp).toInt,
      indexInterval = props.getProperty(IndexIntervalBytesProp).toInt,
      fileDeleteDelayMs = props.getProperty(FileDeleteDelayMsProp).toInt,
      deleteRetentionMs = props.getProperty(DeleteRetentionMsProp).toLong,
      minCleanableRatio = props.getProperty(MinCleanableDirtyRatioProp).toDouble,
      compact = props.getProperty(CleanupPolicyProp).trim.toLowerCase != "delete")
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
    for (name <- props.keys.asScala)
      require(LogConfig.ConfigNames.asJava.contains(name), "Unknown configuration \"%s\".".format(name))
  }

  /**
   * Check that the given properties contain only valid log config names, and that all values can be parsed.
   */
  def validate(props: Properties) {
    validateNames(props)
    LogConfig.fromProps(LogConfig().toProps, props) // check that we can parse the values
  }

  def configNamesAndDoc: Seq[(String, String)] = {
    configNames.map(n => n -> "")
  }
}
