package controllers

import scala.collection._

import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import kafka.api.OffsetRequest
import kafka.consumer.SimpleConsumer
import kafka.utils.Json
import kafka.common.BrokerNotAvailableException
import kafka.utils.ZKStringSerializer
import org.apache.log4j.Logger
import scala.collection.JavaConversions._
import kafka.utils.ZKGroupDirs
import kafka.api.OffsetResponse
import java.util.ArrayList
import scala.collection
import scala.collection.Seq
import scala.collection.Map
import scala.Some
import kafka.common.TopicAndPartition
import kafka.admin.AdminUtils
import kafka.api.PartitionOffsetRequestInfo
import play.api.mvc._

case class Consumer(name: String, groupId: String);
case class ConsumerClusterInfo(consumers: List[String], consumerTopicMap: immutable.Map[String, List[String]])
case class ConsumerOffset(partitionId: Int, owner: String, offset: Long, logSize: Long, lag: Long)
case class ConsumerDetail(topic: String, consumer: String, consumerOffsets: List[ConsumerOffset], totalOffset: Long)

object Consumers extends Controller {
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = KafkaManagerContext.getKafkaManager

  def list(cluster:String) = Action.async { implicit request =>
    kafkaManager.getClusterConfig(cluster).map { errorOrConfig =>
      Ok(views.html.consumer.consumerList(cluster, errorOrConfig.map { clusterConfig => 
        getConsumers(clusterConfig.curatorConfig.zkConnect)}))
    }
  }

  def details(cluster:String, topic: String, consumer: String) = Action.async { implicit request =>
    kafkaManager.getClusterConfig(cluster).map { errorOrConfig =>
      Ok(views.html.consumer.consumerDetail(cluster, consumer, errorOrConfig.map { clusterConfig => 
        getConsumerDetail(topic, consumer, clusterConfig.curatorConfig.zkConnect)}))
     }
  }

  def getConsumers(zkconnect: String): ConsumerClusterInfo = {
    val zkClient: ZkClient = getZkClient(zkconnect);
    try {
      val activeConsumerTopicPair = getActiveConsumerTopicPair(zkClient);
      val consumerTopicMap = activeConsumerTopicPair.groupBy(_._2).mapValues({_.unzip._1}).mapValues(seq => seq toList) toMap
      val consumers = consumerTopicMap.keySet.seq.toList
      ConsumerClusterInfo(consumers, consumerTopicMap)
    }finally{
      closeZkClient(zkClient);
    }
  }
  
  def getActiveConsumerTopicPair(zkClient: ZkClient): collection.Seq[(String,String)] = {
    ZkUtils.getChildren(zkClient,ZkUtils.ConsumersPath).flatMap({
      group =>
        try{
          ZkUtils.getConsumersPerTopic(zkClient,group,true).keySet.map({topic => topic -> group})
        }catch{
          case e:Exception => logger.error("Failed to get topics for consumer group"+ group)
            Seq()
        }
    })
  }
  
  def existsConsumer(zkconnect: String, group: String):Boolean = {
    getConsumerList(zkconnect).contains(group)
  }
  
  def getConsumerList(zkconnect: String):Seq[String] = {
    val zkClient: ZkClient = getZkClient(zkconnect);
    try {
      ZkUtils.getChildren(zkClient, ZkUtils.ConsumersPath).toList;
    } finally{
      closeZkClient(zkClient);
    }
  }
  
  def existsTopic(zkconnect: String, topic: String): Boolean = {
    val zkClient: ZkClient = getZkClient(zkconnect)
    try {
      AdminUtils.topicExists(zkClient,topic)
    } finally{
      closeZkClient(zkClient)
    }
  }
    
  def getConsumerDetail(topic: String, group: String, zkconnect: String): ConsumerDetail = {
    if (!existsTopic(zkconnect, topic)) {
      throw new IllegalArgumentException("Topic " + topic + " does not exists");
    }
    if (!existsConsumer(zkconnect, group)) {
      throw new IllegalArgumentException("Consumer " + group + " does not exists");
    }
    val zkClient: ZkClient = getZkClient(zkconnect);
    try{
        val consumerOffsets =  getConsumerOffset(zkClient, group, topic);
        var total:Long = 0;
        consumerOffsets.foreach(f => total += f.lag);
        ConsumerDetail(topic, group, consumerOffsets, total);
    }finally{
      closeZkClient(zkClient);
    }
  }
  
  def getConsumerOffset(zkClient: ZkClient, group: String, topic: String): List[ConsumerOffset] = {
    val pidMap = ZkUtils.getPartitionsForTopics(zkClient, Seq(topic))
    val consumerMap: mutable.Map[Int, Option[SimpleConsumer]] = mutable.Map()
    try{
      getOffset(zkClient,group,topic,pidMap,consumerMap);
    }finally{
      shutdownConsumer(consumerMap)
    }
  }
  
  private def shutdownConsumer(consumerMap: mutable.Map[Int, Option[SimpleConsumer]]){
      consumerMap.foreach{ c =>
        try{
          c._2 match {
            case Some(m) =>
              m.close()
              logger.debug("Closed consumer for broker: " + c._1)
            case None => logger.error ("Failed to close the consumer for broker:  " + c._1)
          }
        }catch{
          case t:Throwable => logger.error("Failed to close the consumer")
        }
      }
  }
  
  private def getOffset(zkClient:ZkClient,group:String,topic:String, pidMap:Map[String, Seq[scala.Int]],consumerMap: mutable.Map[Int, Option[SimpleConsumer]]) = {
    var consumerOffsets = List[ConsumerOffset]()
    pidMap.get(topic) match {
      case Some(pids) =>
        pids.sorted.foreach {
          partitionId =>
            val cOffset: ConsumerOffset = getConsumerOffset(zkClient, group, topic, partitionId, consumerMap);
            if (cOffset != null) {
              consumerOffsets ::= cOffset
            }
        }
      case None => throw new IllegalArgumentException("No information available for topic: " + topic + " and group: " + group);
    }
    consumerOffsets
  }

  private def getConsumerOffset(zkClient: ZkClient, group: String, topic: String, partitionId: Int, consumerMap: mutable.Map[Int, Option[SimpleConsumer]]): ConsumerOffset = {
    var consumerOffset: ConsumerOffset = null;
    try{
      ZkUtils.readDataMaybeNull(zkClient,"/consumers/%s/offsets/%s".
        format(group, topic))
            val offset = ZkUtils.readData(zkClient, "/consumers/%s/offsets/%s/%s".
              format(group, topic, partitionId))._1.toLong
            val owner = ZkUtils.readDataMaybeNull(zkClient, "/consumers/%s/owners/%s/%s".
              format(group, topic, partitionId))._1
            val topicAndPartition = TopicAndPartition(topic, partitionId)

            ZkUtils.getLeaderForPartition(zkClient, topic, partitionId) match {
              case Some(bid) =>
                val consumerOpt = consumerMap.getOrElseUpdate(bid, getConsumer(zkClient, bid))
                consumerOpt match {
                  case Some(consumer) =>
                    try {
                      val request =
                        OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
                      val offsetResp: OffsetResponse = consumer.getOffsetsBefore(request)
                      logger.debug("Partition offset " + bid + "topic: " + topic + " partition: " + partitionId + " " +  offsetResp.partitionErrorAndOffsets);
                      val logSize = offsetResp.partitionErrorAndOffsets(topicAndPartition).offsets.head
                      val lag = logSize - offset
                      val ownerStr = owner match {
                        case Some(s) => s
                        case None => "none"
                      }
                      consumerOffset = new ConsumerOffset(partitionId, ownerStr, offset, logSize, lag);
                    } catch {
                      case t: Throwable => logger.error("Could not get consumer offset for " + group + " , " + topic + " " + partitionId, t);
                    }
                  case None => logger.debug("No broker available for partitionId: " + partitionId + ", group: " + group + ", topic: " + topic)
                }
              case None =>
                logger.error("No broker for partition %s - %s".format(topic, partitionId))
            }
    }catch {
      case t: Throwable => logger.error("Failed to get offsets for group: " + group + " topic: " + topic, t);
    }
    consumerOffset
  }
    
  private def getConsumer(zkClient: ZkClient, bid: Int): Option[SimpleConsumer] = {
    try {
      ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + bid)._1 match {
        case Some(brokerInfoString) =>
          Json.parseFull(brokerInfoString) match {
            case Some(m) =>
              val brokerInfo = m.asInstanceOf[Map[String, Any]]
              val host = brokerInfo.get("host").get.asInstanceOf[String]
              val port = brokerInfo.get("port").get.asInstanceOf[Int]
              Some(new SimpleConsumer(host, port, 10000, 100000, "ConsumerOffsetChecker"))
            case None =>
              throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
          }
        case None =>
          throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
      }
    } catch {
      case t: Throwable =>
        logger.error("Could not parse broker info for bid: " + bid)
        logger.debug("Could not parse broker info", t)
        None
    }
  }
    
  def getZkClient(zkConnect: String) = {
      new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer);
  }
  
  def closeZkClient(zkClient:ZkClient){
      if (zkClient != null)
      zkClient.close()
  }
}
