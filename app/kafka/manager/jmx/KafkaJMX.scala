/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.jmx

import com.yammer.metrics.reporting.JmxReporter.GaugeMBean
import grizzled.slf4j.Logging
import kafka.manager.model.ActorModel.BrokerMetrics
import kafka.manager.model.{KafkaVersion, Kafka_0_8_1_1}

import java.io.File
import java.{util => ju}
import javax.management._
import javax.management.remote.rmi.RMIConnectorServer
import javax.management.remote.{JMXConnector, JMXConnectorFactory, JMXServiceURL}
import javax.naming.Context
import javax.rmi.ssl.SslRMIClientSocketFactory
import scala.collection.JavaConverters._
import scala.util.matching.Regex
import scala.util.{Failure, Try}

object KafkaJMX extends Logging {
  
  private[this] val defaultJmxConnectorProperties = Map[String, Any] (
    "jmx.remote.x.request.waiting.timeout" -> "3000",
    "jmx.remote.x.notification.fetch.timeout" -> "3000",
    "sun.rmi.transport.connectionTimeout" -> "3000",
    "sun.rmi.transport.tcp.handshakeTimeout" -> "3000",
    "sun.rmi.transport.tcp.responseTimeout" -> "3000"
  )

  def doWithConnection[T](jmxHost: String, jmxPort: Int, jmxUser: Option[String], jmxPass: Option[String], jmxSsl: Boolean)(fn: MBeanServerConnection => T) : Try[T] = {
    val urlString = s"service:jmx:rmi:///jndi/rmi://$jmxHost:$jmxPort/jmxrmi"
    val url = new JMXServiceURL(urlString)
    try {
      require(jmxPort > 0, "No jmx port but jmx polling enabled!")
      val credsProps: Option[Map[String, _]] = for {
        user <- jmxUser
        pass <- jmxPass
      } yield {
        Map(JMXConnector.CREDENTIALS -> Array(user, pass))
      }
      val sslProps: Option[Map[String, _]] = if (jmxSsl) {
        val clientSocketFactory = new SslRMIClientSocketFactory()
        Some(Map(
          Context.SECURITY_PROTOCOL -> "ssl",
          RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE -> clientSocketFactory,
          "com.sun.jndi.rmi.factory.socket" -> clientSocketFactory
        ))
      } else {
        None
      }
      val jmxConnectorProperties = List(credsProps, sslProps).flatten.foldRight(defaultJmxConnectorProperties)(_ ++ _)
      val jmxc = JMXConnectorFactory.connect(url, jmxConnectorProperties.asJava)
      try {
        Try {
          fn(jmxc.getMBeanServerConnection)
        }
      } finally {
        jmxc.close()
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to connect to $urlString",e)
        Failure(e)
    }
  }
}

object KafkaMetrics {

  def getBytesInPerSec(kafkaVersion: KafkaVersion, mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(kafkaVersion, mbsc, "BytesInPerSec", topicOption)
  }

  def getBytesOutPerSec(kafkaVersion: KafkaVersion, mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(kafkaVersion, mbsc, "BytesOutPerSec", topicOption)
  }

  def getBytesRejectedPerSec(kafkaVersion: KafkaVersion, mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(kafkaVersion, mbsc, "BytesRejectedPerSec", topicOption)
  }

  def getFailedFetchRequestsPerSec(kafkaVersion: KafkaVersion, mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(kafkaVersion, mbsc, "FailedFetchRequestsPerSec", topicOption)
  }

  def getFailedProduceRequestsPerSec(kafkaVersion: KafkaVersion, mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(kafkaVersion, mbsc, "FailedProduceRequestsPerSec", topicOption)
  }

  def getMessagesInPerSec(kafkaVersion: KafkaVersion, mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(kafkaVersion, mbsc, "MessagesInPerSec", topicOption)
  }

  private def getBrokerTopicMeterMetrics(kafkaVersion: KafkaVersion, mbsc: MBeanServerConnection, metricName: String, topicOption: Option[String]) = {
    getMeterMetric(mbsc, getObjectName(kafkaVersion, metricName, topicOption))
  }
  
  private def getSep(kafkaVersion: KafkaVersion) : String = {
    kafkaVersion match {
      case Kafka_0_8_1_1 => "\""
      case _ => ""
    }
  }

  def getObjectName(kafkaVersion: KafkaVersion, name: String, topicOption: Option[String] = None) = {
    val sep = getSep(kafkaVersion)
    val topicAndName = kafkaVersion match {
      case Kafka_0_8_1_1 => 
        topicOption.map( topic => s"${sep}$topic-$name${sep}").getOrElse(s"${sep}AllTopics$name${sep}")
      case _ =>
        val topicProp = topicOption.map(topic => s",topic=$topic").getOrElse("")
        s"$name$topicProp"
    }
    new ObjectName(s"${sep}kafka.server${sep}:type=${sep}BrokerTopicMetrics${sep},name=$topicAndName")
  }


  /* Gauge, Value : 0 */
  private val replicaFetcherManagerMinFetchRate = new ObjectName(
    "kafka.server:type=ReplicaFetcherManager,name=MinFetchRate,clientId=Replica")

  /* Gauge, Value : 0 */
  private val replicaFetcherManagerMaxLag = new ObjectName(
    "kafka.server:type=ReplicaFetcherManager,name=MaxLag,clientId=Replica")
  
  /* Gauge, Value : 0 */
  private val kafkaControllerActiveControllerCount = new ObjectName(
    "kafka.controller:type=KafkaController,name=ActiveControllerCount")
  
  /* Gauge, Value : 0 */
  private val kafkaControllerOfflinePartitionsCount = new ObjectName(
    "kafka.controller:type=KafkaController,name=OfflinePartitionsCount")

  /* Timer*/
  private val logFlushStats = new ObjectName(
    "kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs")

  /* Operating System */
  private val operatingSystemObjectName = new ObjectName("java.lang:type=OperatingSystem")

  /* Log Segments */
  private val logSegmentObjectName = new ObjectName("kafka.log:type=Log,name=*-LogSegments")

  private val directoryObjectName = new ObjectName("kafka.log:type=Log,name=*-Directory")

  private val LogSegmentsNameRegex = new Regex("%s-LogSegments".format("""(.*)-(\d*)"""), "topic", "partition")

  private val DirectoryNameRegex = new Regex("%s-Directory".format("""(.*)-(\d*)"""), "topic", "partition")

  val LogSegmentRegex = new Regex(
    "baseOffset=(.*), created=(.*), logSize=(.*), indexSize=(.*)",
    "baseOffset", "created", "logSize", "indexSize"
  )

  private def getOSMetric(mbsc: MBeanServerConnection) = {
    import scala.collection.JavaConverters._
    try {
      val attributes = mbsc.getAttributes(
        operatingSystemObjectName,
        Array("ProcessCpuLoad", "SystemCpuLoad")
      ).asList().asScala.toSeq
      OSMetric(
        getDoubleValue(attributes, "ProcessCpuLoad"),
        getDoubleValue(attributes, "SystemCpuload")
      )
    } catch {
      case _: InstanceNotFoundException => OSMetric(0D, 0D)
    }
  }
  
  private def getMeterMetric(mbsc: MBeanServerConnection, name: ObjectName) = {
    import scala.collection.JavaConverters._
    try {
      val attributeList = mbsc.getAttributes(name, Array("Count", "FifteenMinuteRate", "FiveMinuteRate", "OneMinuteRate", "MeanRate"))
      val attributes = attributeList.asList().asScala.toSeq
      MeterMetric(getLongValue(attributes, "Count"),
        getDoubleValue(attributes, "FifteenMinuteRate"),
        getDoubleValue(attributes, "FiveMinuteRate"),
        getDoubleValue(attributes, "OneMinuteRate"),
        getDoubleValue(attributes, "MeanRate"))
    } catch {
        case _: InstanceNotFoundException => MeterMetric(0,0,0,0,0)
      }
  }
  
  private def getLongValue(attributes: Seq[Attribute], name: String) = {
    attributes.find(_.getName == name).map(_.getValue.asInstanceOf[Long]).getOrElse(0L)
  }

  private def getDoubleValue(attributes: Seq[Attribute], name: String) = {
    attributes.find(_.getName == name).map(_.getValue.asInstanceOf[Double]).getOrElse(0D)
  }

  private def topicAndPartition(name: String, regex: Regex) = {
    try {
      val matches = regex.findAllIn(name).matchData.toSeq
      require(matches.size == 1)
      val m = matches.head

      val topic = m.group("topic")
      val partition = m.group("partition").toInt

      (topic, partition)
    }
    catch {
      case e: Exception =>
        throw new IllegalStateException("Can't parse topic and partition from: <%s>".format(name), e)
    }
  }

  private def queryValues[K, V](
    mbsc: MBeanServerConnection,
    objectName: ObjectName,
    keyConverter: String => K,
    valueConverter: Object => V
    ) = {
    val logsSizeObjectNames = mbsc.queryNames(objectName, null).asScala.toSeq
    logsSizeObjectNames.par.map {
      objectName => queryValue(mbsc, objectName, keyConverter, valueConverter)
    }.seq.toSeq
  }

  private def queryValue[K, V](
    mbsc: MBeanServerConnection,
    objectName: ObjectName,
    keyConverter: String => K,
    valueConverter: Object => V
    ) = {
    val name = objectName.getKeyProperty("name")
    val mbean = MBeanServerInvocationHandler.newProxyInstance(mbsc, objectName, classOf[GaugeMBean], true)
    (keyConverter(name), valueConverter(mbean.getValue))
  }

  private def parseLogSegment(str: String): LogSegment = {
    try {
      val matches = LogSegmentRegex.findAllIn(str).matchData.toSeq
      require(matches.size == 1)
      val m = matches.head

      LogSegment(
        baseOffset = m.group("baseOffset").toLong,
        created = m.group("created").toLong,
        logBytes = m.group("logSize").toLong,
        indexBytes = m.group("indexSize").toLong
      )
    } catch {
      case e: Exception =>
        throw new IllegalStateException("Can't parse segment info from: <%s>".format(str), e)
    }
  }

  def getLogSegmentsInfo(mbsc: MBeanServerConnection) = {
    val logSegmentsMap = {
      queryValues(
        mbsc,
        logSegmentObjectName,
        key => topicAndPartition(key, LogSegmentsNameRegex),
        value => {
          val lst = value.asInstanceOf[ju.List[String]]
          lst.asScala.map(parseLogSegment).toSeq
        }
      )
    }.toMap

    val directoryMap = {
      queryValues(
        mbsc,
        directoryObjectName,
        key => topicAndPartition(key, DirectoryNameRegex),
        value => value.asInstanceOf[String]
      )
    }.toMap

    val stats: Seq[(String, (Int, LogInfo))] = for (
      key <- (logSegmentsMap.keySet ++ directoryMap.keySet).toSeq;
      directory <- directoryMap.get(key);
      logSegments <- logSegmentsMap.get(key)
    ) yield {
        val directoryFile = new File(directory)
        val dir = directoryFile.getParentFile.getAbsolutePath

        val (topic, partition) = key

        (topic, (partition, LogInfo(dir, logSegments)))
      }

    stats.groupBy(_._1).mapValues(_.map(_._2).toMap).toMap
  }

  // return broker metrics with segment metric only when it's provided. if not, it will contain segment metric with value 0L
  def getBrokerMetrics(kafkaVersion: KafkaVersion, mbsc: MBeanServerConnection, segmentsMetric: Option[SegmentsMetric] = None, topic: Option[String] = None) : BrokerMetrics = {
    BrokerMetrics(
      KafkaMetrics.getBytesInPerSec(kafkaVersion, mbsc, topic),
      KafkaMetrics.getBytesOutPerSec(kafkaVersion, mbsc, topic),
      KafkaMetrics.getBytesRejectedPerSec(kafkaVersion, mbsc, topic),
      KafkaMetrics.getFailedFetchRequestsPerSec(kafkaVersion, mbsc, topic),
      KafkaMetrics.getFailedProduceRequestsPerSec(kafkaVersion, mbsc, topic),
      KafkaMetrics.getMessagesInPerSec(kafkaVersion, mbsc, topic),
      KafkaMetrics.getOSMetric(mbsc),
      segmentsMetric.getOrElse(SegmentsMetric(0L))
    )
  }
}

case class GaugeMetric(value: Double)

case class OSMetric(processCpuLoad: Double,
                    systemCpuLoad: Double) {

  def formatProcessCpuLoad = {
    FormatMetric.rateFormat(processCpuLoad, 0)
  }

  def formatSystemCpuLoad = {
    FormatMetric.rateFormat(systemCpuLoad, 0)
  }
}

case class SegmentsMetric(bytes: Long) {
  def +(o: SegmentsMetric) : SegmentsMetric = {
    SegmentsMetric(o.bytes + bytes)
  }

  def formatSize = {
    FormatMetric.sizeFormat(bytes)
  }
}

case class MeterMetric(count: Long,
                      fifteenMinuteRate: Double,
                      fiveMinuteRate: Double,
                      oneMinuteRate: Double,
                      meanRate: Double) {

  def formatFifteenMinuteRate = {
    FormatMetric.rateFormat(fifteenMinuteRate, 0)
  }

  def formatFiveMinuteRate = {
    FormatMetric.rateFormat(fiveMinuteRate, 0)
  }

  def formatOneMinuteRate = {
    FormatMetric.rateFormat(oneMinuteRate, 0)
  }

  def formatMeanRate = {
    FormatMetric.rateFormat(meanRate, 0)
  }

  def +(o: MeterMetric) : MeterMetric = {
    MeterMetric(
      o.count + count, 
      o.fifteenMinuteRate + fifteenMinuteRate, 
      o.fiveMinuteRate + fiveMinuteRate, 
      o.oneMinuteRate + oneMinuteRate, 
      o.meanRate + meanRate)
  }
}

case class LogInfo(dir: String, logSegments: Seq[LogSegment]) {

  val bytes = logSegments.map(_.bytes).sum
}

case class LogSegment(
  baseOffset: Long,
  created: Long,
  logBytes: Long,
  indexBytes: Long) {

  val bytes = logBytes + indexBytes
}

object FormatMetric {
  private[this] val UNIT = Array[Char]('k', 'm', 'b', 't')

  // See: http://stackoverflow.com/a/4753866
  def rateFormat(rate: Double, iteration: Int): String = {
    if (rate < 100) {
      BigDecimal(rate).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString
    } else {
      val value = (rate.toLong / 100) / 10.0
      val isRound: Boolean = (value * 10) % 10 == 0 //true if the decimal part is equal to 0 (then it's trimmed anyway)
      if (value < 1000) {
        //this determines the class, i.e. 'k', 'm' etc
        if (value > 99.9 || isRound || (!isRound && value > 9.99)) {
          //this decides whether to trim the decimals
          value.toInt * 10 / 10 + "" + UNIT(iteration) // (int) value * 10 / 10 drops the decimal
        }
        else {
          value + "" + UNIT(iteration)
        }
      }
      else {
        rateFormat(value, iteration + 1)
      }
    }
  }

  // See: http://stackoverflow.com/a/3758880
  def sizeFormat(bytes: Long): String = {
    val unit = 1000
    if (bytes < unit) {
      bytes + " B"
    } else {
      val exp = (math.log(bytes) / math.log(unit)).toInt
      val pre = "kMGTPE".charAt(exp-1)
      "%.1f %sB".format(bytes / math.pow(unit, exp), pre)
    }
  }
}
