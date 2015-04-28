package kafka.manager

import javax.management._
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}

import kafka.manager.ActorModel.BrokerMetrics
import org.slf4j.LoggerFactory

import scala.util.{Failure, Try}

object KafkaJMX {
  
  private[this] lazy val logger = LoggerFactory.getLogger(this.getClass)

  def doWithConnection[T](jmxHost: String, jmxPort: Int)(fn: MBeanServerConnection => T) : Try[T] = {
    val urlString = s"service:jmx:rmi:///jndi/rmi://$jmxHost:$jmxPort/jmxrmi"
    val url = new JMXServiceURL(urlString)
    try {
      require(jmxPort > 0, "No jmx port but jmx polling enabled!")
      val jmxc = JMXConnectorFactory.connect(url, null)
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

  def getBytesInPerSec(mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(mbsc, "BytesInPerSec", topicOption)
  }

  def getBytesOutPerSec(mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(mbsc, "BytesOutPerSec", topicOption)
  }

  def getBytesRejectedPerSec(mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(mbsc, "BytesRejectedPerSec", topicOption)
  }

  def getFailedFetchRequestsPerSec(mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(mbsc, "FailedFetchRequestsPerSec", topicOption)
  }

  def getFailedProduceRequestsPerSec(mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(mbsc, "FailedProduceRequestsPerSec", topicOption)
  }

  def getMessagesInPerSec(mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(mbsc, "MessagesInPerSec", topicOption)
  }

  private def getBrokerTopicMeterMetrics(mbsc: MBeanServerConnection, metricName: String, topicOption: Option[String]) = {
    getMeterMetric(mbsc, getObjectName(metricName, topicOption))
  }

  private def getObjectName(name: String, topicOption: Option[String] = None) = {
    val topicProp = topicOption.map(topic => s",topic=$topic").getOrElse("")
    new ObjectName(s"kafka.server:type=BrokerTopicMetrics,name=$name$topicProp")
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
  
  private def getMeterMetric(mbsc: MBeanServerConnection, name:ObjectName) = {
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

  def getBrokerMetrics(mbsc: MBeanServerConnection, topic: Option[String] = None) : BrokerMetrics = {
    BrokerMetrics(
      KafkaMetrics.getBytesInPerSec(mbsc, topic),
      KafkaMetrics.getBytesOutPerSec(mbsc, topic),
      KafkaMetrics.getBytesRejectedPerSec(mbsc, topic),
      KafkaMetrics.getFailedFetchRequestsPerSec(mbsc, topic),
      KafkaMetrics.getFailedProduceRequestsPerSec(mbsc, topic),
      KafkaMetrics.getMessagesInPerSec(mbsc, topic))
  }
}

case class GaugeMetric(value: Double)

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
}