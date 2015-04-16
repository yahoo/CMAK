/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

import collection.JavaConversions._
import controllers.KafkaManagerContext
import kafka.manager.KafkaManager
import play.api._
import com.typesafe.config.ConfigException
import models.navigation.BreadCrumbs

/**
 * @author hiral
 */
object Global extends GlobalSettings {

  private[this] var kafkaManager: KafkaManager = null
  val ClusterZks = "managed-kafka-clusters"

  override def beforeStart(app: Application): Unit = {
    Logger.info("Init kafka manager...")
    kafkaManager = KafkaManagerContext.getKafkaManger
    initialize_with_clusters_from_conf()
    Thread.sleep(5000)
  }

  override def onStop(app: Application) {
    KafkaManagerContext.shutdown()
    Logger.info("Application shutdown...")
  }
  

  def initialize_with_clusters_from_conf() {
    Logger.info("initialize_with_clusters_from_conf(): Going to read kafka list from config and initialize")
    
    val configWithDefaults = kafkaManager.getConfigWithDefaults
    
    var member_zkhosts_list =  List[String]()
    try {
      member_zkhosts_list = configWithDefaults.getStringList(ClusterZks).toList
    } catch {
      case cfge: ConfigException => Logger.error("managed-kafka-clusters param not defined in the conf or is in wrong format")
    }
    
    
    member_zkhosts_list.foreach{ item => 
      
      // Each item in the list from config will be a string of format
      // "clustername, version, zk1:port1, zk2:port2...."
      //
      val mylist = item.split(",")
      val name = mylist(0)
      val version = mylist(1)
      val zkhosts = mylist.takeRight(mylist.size - 2).mkString(",")

      
      kafkaManager.addCluster(name, version, zkhosts)
      
    }
  }
  
  
}


