package kafka.manager.utils

import java.util.Properties

import kafka.manager.ActorModel.BrokerIdentity
import kafka.producer.{KeyedMessage, NewShinyProducer}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer

/**
 * Borrowed from kafka 0.8.2.1
 * https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/tools/ConsoleProducer.scala
 */
object KafkaMessageProducer {

  def send(message: String, config: MessageProducerConfig) {
    val producer = new NewShinyProducer(getProducerProps(config))
    try {
      val keyedMessage = new KeyedMessage[Array[Byte], Array[Byte]](config.topic, message.getBytes)
      producer.send(keyedMessage.topic, keyedMessage.key, keyedMessage.message)
    } finally {
      producer.close()
    }
  }

  def getProducerProps(config: MessageProducerConfig): Properties = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokerList.map(b => b.host + ":" + b.port).mkString(","))
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-manager-producer")
    val bytArraySerializerName = classOf[ByteArraySerializer].getName
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, bytArraySerializerName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, bytArraySerializerName)
    props
  }

  case class MessageProducerConfig(topic: String, brokerList: Seq[BrokerIdentity])

}