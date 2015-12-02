package kafka.manager.utils.zero82

import java.io.IOException

import grizzled.slf4j.Logging
import kafka.api.{ConsumerMetadataResponse, ConsumerMetadataRequest}
import kafka.cluster.Broker
import kafka.common.ErrorMapping
import kafka.network.BlockingChannel

import scala.util.Random

/**
 * Borrowed from kafka 0.8.2.1
 * https://git-wip-us.apache.org/repos/asf?p=kafka.git;a=blob;f=clients/src/main/java/org/apache/kafka/common/utils/ClientUtils.java
 */
object ClientUtils extends Logging {

  /**
   * Creates a blocking channel to a random broker
   */
  def channelToAnyBroker(allBrokers: Seq[Broker], socketTimeoutMs: Int = 3000) : BlockingChannel = {
    var channel: BlockingChannel = null
    var connected = false
    while (!connected) {
      Random.shuffle(allBrokers).find { broker =>
        trace("Connecting to broker %s:%d.".format(broker.host, broker.port))
        try {
          channel = new BlockingChannel(broker.host, broker.port, BlockingChannel.UseDefaultBufferSize, BlockingChannel.UseDefaultBufferSize, socketTimeoutMs)
          channel.connect()
          debug("Created channel to broker %s:%d.".format(channel.host, channel.port))
          true
        } catch {
          case e: Exception =>
            if (channel != null) channel.disconnect()
            channel = null
            info("Error while creating channel to %s:%d.".format(broker.host, broker.port))
            false
        }
      }
      connected = if (channel == null) false else true
    }

    channel
  }
  
  /**
   * Creates a blocking channel to the offset manager of the given group
   */
  def channelToOffsetManager(group: String, allBrokers: Seq[Broker], socketTimeoutMs: Int = 3000, retryBackOffMs: Int = 1000) = {
    var queryChannel = channelToAnyBroker(allBrokers)

    var offsetManagerChannelOpt: Option[BlockingChannel] = None

    while (!offsetManagerChannelOpt.isDefined) {

      var coordinatorOpt: Option[Broker] = None

      while (!coordinatorOpt.isDefined) {
        try {
          if (!queryChannel.isConnected)
            queryChannel = channelToAnyBroker(allBrokers)
          debug("Querying %s:%d to locate offset manager for %s.".format(queryChannel.host, queryChannel.port, group))
          queryChannel.send(ConsumerMetadataRequest(group))
          val response = queryChannel.receive()
          val consumerMetadataResponse =  ConsumerMetadataResponse.readFrom(response.buffer)
          debug("Consumer metadata response: " + consumerMetadataResponse.toString)
          if (consumerMetadataResponse.errorCode == ErrorMapping.NoError)
            coordinatorOpt = consumerMetadataResponse.coordinatorOpt
          else {
            debug("Query to %s:%d to locate offset manager for %s failed - will retry in %d milliseconds."
              .format(queryChannel.host, queryChannel.port, group, retryBackOffMs))
            Thread.sleep(retryBackOffMs)
          }
        }
        catch {
          case ioe: IOException =>
            info("Failed to fetch consumer metadata from %s:%d.".format(queryChannel.host, queryChannel.port))
            queryChannel.disconnect()
        }
      }

      val coordinator = coordinatorOpt.get
      if (coordinator.host == queryChannel.host && coordinator.port == queryChannel.port) {
        offsetManagerChannelOpt = Some(queryChannel)
      } else {
        val connectString = "%s:%d".format(coordinator.host, coordinator.port)
        var offsetManagerChannel: BlockingChannel = null
        try {
          debug("Connecting to offset manager %s.".format(connectString))
          offsetManagerChannel = new BlockingChannel(coordinator.host, coordinator.port,
            BlockingChannel.UseDefaultBufferSize,
            BlockingChannel.UseDefaultBufferSize,
            socketTimeoutMs)
          offsetManagerChannel.connect()
          offsetManagerChannelOpt = Some(offsetManagerChannel)
          queryChannel.disconnect()
        }
        catch {
          case ioe: IOException => // offsets manager may have moved
            info("Error while connecting to %s.".format(connectString))
            if (offsetManagerChannel != null) offsetManagerChannel.disconnect()
            Thread.sleep(retryBackOffMs)
            offsetManagerChannelOpt = None // just in case someone decides to change shutdownChannel to not swallow exceptions
        }
      }
    }

    offsetManagerChannelOpt.get
  }

}
