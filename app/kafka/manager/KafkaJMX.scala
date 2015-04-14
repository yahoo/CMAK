package kafka.manager

import javax.management._
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}

import scala.util.Try

object KafkaJMX {

  def connect(jmxHost: String, jmxPort: Int): Try[MBeanServerConnection] = {
    Try {
      val url = new JMXServiceURL(s"service:jmx:rmi:///jndi/rmi://$jmxHost:$jmxPort/jmxrmi")
      val jmxc = JMXConnectorFactory.connect(url, null)
      jmxc.getMBeanServerConnection
    }
  }
}

object KafkaMetrics {

  def getBytesInPerSec(mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicRateMetrics(mbsc, "BytesInPerSec", topicOption)
  }

  def getBytesOutPerSec(mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicRateMetrics(mbsc, "BytesOutPerSec", topicOption)
  }

  def getBytesRejectedPerSec(mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicRateMetrics(mbsc, "BytesRejectedPerSec", topicOption)
  }

  def getFailedFetchRequestsPerSec(mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicRateMetrics(mbsc, "FailedFetchRequestsPerSec", topicOption)
  }

  def getFailedProduceRequestsPerSec(mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicRateMetrics(mbsc, "FailedProduceRequestsPerSec", topicOption)
  }

  def getMessagesInPerSec(mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicRateMetrics(mbsc, "MessagesInPerSec", topicOption)
  }

  private def getBrokerTopicRateMetrics(mbsc: MBeanServerConnection, metricName: String, topicOption: Option[String]) = {
    getRateMetric(mbsc, getObjectName(metricName, topicOption))
  }

  private def getObjectName(name: String, topicOption: Option[String] = None) = {
    val topicProp = topicOption.map(topic => s",topic=$topic").getOrElse("")
    new ObjectName(s"kafka.server:type=BrokerTopicMetrics,name=$name$topicProp")
  }

  private def getRateMetric(mbsc: MBeanServerConnection, name:ObjectName) = {
    import scala.collection.JavaConverters._
    try {
      val attributeList = mbsc.getAttributes(name, Array("Count", "FifteenMinuteRate", "FiveMinuteRate", "OneMinuteRate", "MeanRate"))
      val attributes = attributeList.asList().asScala.toSeq
      RateMetric(getLongValue(attributes, "Count"),
        getDoubleValue(attributes, "FifteenMinuteRate"),
        getDoubleValue(attributes, "FiveMinuteRate"),
        getDoubleValue(attributes, "OneMinuteRate"),
        getDoubleValue(attributes, "MeanRate"))
    } catch {
        case _: InstanceNotFoundException => RateMetric(0,0,0,0,0)
      }
  }

  private def getLongValue(attributes: Seq[Attribute], name: String) = {
    attributes.find(_.getName == name).map(_.getValue.asInstanceOf[Long]).getOrElse(0L)
  }

  private def getDoubleValue(attributes: Seq[Attribute], name: String) = {
    attributes.find(_.getName == name).map(_.getValue.asInstanceOf[Double]).getOrElse(0D)
  }
}

case class RateMetric(count: Long,
                      fifteenMinuteRate: Double,
                      fiveMinuteRate: Double,
                      oneMinuteRate: Double,
                      meanRate: Double) {

  val UNIT = Array[Char]('k', 'm', 'b', 't')

  def formatFifteenMinuteRate = {
    rateFormat(fifteenMinuteRate, 0)
  }

  def formatFiveMinuteRate = {
    rateFormat(fiveMinuteRate, 0)
  }

  def formatOneMinuteRate = {
    rateFormat(oneMinuteRate, 0)
  }

  def formatMeanRate = {
    rateFormat(meanRate, 0)
  }

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