/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package models.navigation

import features.{KMTopicManagerFeature, KMClusterManagerFeature, ApplicationFeatures}
import kafka.manager.features.{KMLogKafkaFeature, ClusterFeatures}

/**
 * @author hiral
 */
object Menus {
  import models.navigation.QuickRoutes._
  
  private[this] def clusterMenu(cluster: String, applicationFeatures: ApplicationFeatures) : Option[Menu] = {
    val defaultItems = IndexedSeq("Summary".clusterRouteMenuItem(cluster),
                                  "List".baseRouteMenuItem)
    val items = {
      if(applicationFeatures.features(KMClusterManagerFeature))
        defaultItems.+:("Add Cluster".baseRouteMenuItem)
      else
        defaultItems
    }
    
    Option(Menu("Cluster", items, None))
  }

  private[this] def topicMenu(cluster: String, applicationFeatures: ApplicationFeatures) : Option[Menu] = {
    val defaultItems = IndexedSeq("List".clusterRouteMenuItem(cluster))
    
    val items = {
      if(applicationFeatures.features(KMTopicManagerFeature))
        defaultItems.+:("Create".clusterRouteMenuItem(cluster))
      else
        defaultItems
    }

    Option(Menu("Topic", items, None))
  }
  
  private[this] def brokersMenu(cluster: String, applicationFeatures: ApplicationFeatures) : Option[Menu] = {
    Option("Brokers".clusterMenu(cluster))
  }
  
  private[this] def preferredReplicaElectionMenu(cluster: String, applicationFeatures: ApplicationFeatures) : Option[Menu] = {
    Option("Preferred Replica Election".clusterMenu(cluster))
  }
  
  private[this] def reassignPartitionsMenu(cluster: String, applicationFeatures: ApplicationFeatures) : Option[Menu] = {
    Option("Reassign Partitions".clusterMenu(cluster))
  }

  private[this] def consumersMenu(cluster: String, applicationFeatures: ApplicationFeatures) : Option[Menu] = {
    Option("Consumers".clusterMenu(cluster))
  }
  
  private[this] def logKafkaMenu(cluster: String, 
                                 applicationFeatures: ApplicationFeatures, 
                                 clusterFeatures: ClusterFeatures) : Option[Menu] = {
    if (clusterFeatures.features(KMLogKafkaFeature)) {
      Option(Menu("Logkafka", IndexedSeq(
        "List Logkafka".clusterRouteMenuItem(cluster),
        "Create Logkafka".clusterRouteMenuItem(cluster)),
                  None))
    } else None
  }
  
  def clusterMenus(cluster: String)
                  (implicit applicationFeatures: ApplicationFeatures,
                   clusterFeatures: ClusterFeatures) : IndexedSeq[Menu] = {
    IndexedSeq(
      clusterMenu(cluster, applicationFeatures),
      brokersMenu(cluster, applicationFeatures),
      topicMenu(cluster, applicationFeatures),
      preferredReplicaElectionMenu(cluster, applicationFeatures),
      reassignPartitionsMenu(cluster, applicationFeatures),
      consumersMenu(cluster, applicationFeatures),
      logKafkaMenu(cluster, applicationFeatures, clusterFeatures)
    ).flatten
  }
  
  val indexMenu = {
    val defaultItems = IndexedSeq("List".baseRouteMenuItem)
    val items = {
      if(ApplicationFeatures.features.features(KMClusterManagerFeature))
        defaultItems.+:("Add Cluster".baseRouteMenuItem)
      else
        defaultItems
    }
    IndexedSeq(Menu("Cluster", items, None))
  }
}
