/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package models.navigation

import features._
import kafka.manager.features.{ClusterFeatures, KMLogKafkaFeature}

/**
 * @author hiral
 */
class Menus(implicit applicationFeatures: ApplicationFeatures) {
  import models.navigation.QuickRoutes._
  
  private[this] def clusterMenu(cluster: String) : Option[Menu] = {
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

  private[this] def topicMenu(cluster: String) : Option[Menu] = {
    val defaultItems = IndexedSeq("List".clusterRouteMenuItem(cluster))
    
    val items = {
      if(applicationFeatures.features(KMTopicManagerFeature))
        defaultItems.+:("Create".clusterRouteMenuItem(cluster))
      else
        defaultItems
    }

    Option(Menu("Topic", items, None))
  }
  
  private[this] def brokersMenu(cluster: String) : Option[Menu] = {
    Option("Brokers".clusterMenu(cluster))
  }
  
  private[this] def preferredReplicaElectionMenu(cluster: String) : Option[Menu] = {
    if (applicationFeatures.features(KMPreferredReplicaElectionFeature)) {
      Option("Preferred Replica Election".clusterMenu(cluster))
    } else None
  }
  
  private[this] def scheduleLeaderElectionMenu(cluster: String) : Option[Menu] = {
    if (applicationFeatures.features(KMScheduleLeaderElectionFeature)) {
      Option("Schedule Leader Election".clusterMenu(cluster))
    } else None
  }

  private[this] def reassignPartitionsMenu(cluster: String) : Option[Menu] = {
    if (applicationFeatures.features(KMReassignPartitionsFeature)) {
      Option("Reassign Partitions".clusterMenu(cluster))
    } else None
  }

  private[this] def consumersMenu(cluster: String) : Option[Menu] = {
    Option("Consumers".clusterMenu(cluster))
  }
  
  private[this] def logKafkaMenu(cluster: String, 
                                 clusterFeatures: ClusterFeatures) : Option[Menu] = {
    if (clusterFeatures.features(KMLogKafkaFeature)) {
      Option(Menu("Logkafka", IndexedSeq(
        "List Logkafka".clusterRouteMenuItem(cluster),
        "Create Logkafka".clusterRouteMenuItem(cluster)),
                  None))
    } else None
  }
  
  def clusterMenus(cluster: String)
                  (implicit clusterFeatures: ClusterFeatures) : IndexedSeq[Menu] = {
    IndexedSeq(
      clusterMenu(cluster),
      brokersMenu(cluster),
      topicMenu(cluster),
      preferredReplicaElectionMenu(cluster),
      scheduleLeaderElectionMenu(cluster),
      reassignPartitionsMenu(cluster),
      consumersMenu(cluster),
      logKafkaMenu(cluster, clusterFeatures)
    ).flatten
  }
  
  def indexMenu = {
    val defaultItems = IndexedSeq("List".baseRouteMenuItem)
    val items = {
      if(applicationFeatures.features(KMClusterManagerFeature))
        defaultItems.+:("Add Cluster".baseRouteMenuItem)
      else
        defaultItems
    }
    IndexedSeq(Menu("Cluster", items, None))
  }
}
