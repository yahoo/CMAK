/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package models.navigation

import play.api.mvc.Call

/**
 * @author hiral
 */
object QuickRoutes {
  import models.navigation.BreadCrumbs._

  val baseRoutes : Map[String, Call] = Map(
    "Clusters" -> controllers.routes.Application.index(),
    "Schedulers" -> controllers.routes.Application.index(),
    "List" -> controllers.routes.Application.index(),
    "Add Cluster" -> controllers.routes.Cluster.addCluster(),
    "Add Scheduler" -> controllers.routes.Cluster.addScheduler()
  )
  val clusterRoutes : Map[String, String => Call] = Map(
    "Update Cluster" -> controllers.routes.Cluster.updateCluster,
    "Summary" -> controllers.routes.Application.cluster,
    "Brokers" -> controllers.routes.Application.brokers,
    "Topics" -> controllers.routes.Topic.topics,
    "List" -> controllers.routes.Topic.topics,
    "Create" -> controllers.routes.Topic.createTopic,
    "Preferred Replica Election" -> controllers.routes.PreferredReplicaElection.preferredReplicaElection,
    "Reassign Partitions" -> controllers.routes.ReassignPartitions.reassignPartitions
  )

  val schedulerRoutes : Map[String, String => Call] = Map(
    "Update Scheduler" -> controllers.routes.Cluster.updateCluster,
    "Summary" -> scheduler.controllers.routes.SchedulerApplication.getScheduler,
    "Brokers" -> scheduler.controllers.routes.SchedulerApplication.brokers,
    "Add Broker" -> scheduler.controllers.routes.Broker.addBroker,
    "Rebalance Topics" -> scheduler.controllers.routes.RebalanceTopics.rebalanceTopics
  )

  val topicRoutes : Map[String, (String, String) => Call] = Map(
    "Topic View" -> controllers.routes.Topic.topic,
    "Add Partitions" -> controllers.routes.Topic.addPartitions,
    "Update Config" -> controllers.routes.Topic.addPartitions
  )

  implicit class BaseRoute(s: String) {
    def baseRouteMenuItem : (String, Call) = {
      s -> baseRoutes(s)
    }
    def baseRoute : Call = {
      baseRoutes(s)
    }
    def baseMenu(c: String): Menu = {
      Menu(s,IndexedSeq.empty,Some(baseRoute))
    }
    def baseRouteBreadCrumb : BCStaticLink = {
      BCStaticLink(s, baseRoutes(s))
    }
  }

  implicit class ClusterRoute(s: String) {
    def clusterRouteMenuItem(c: String): (String, Call) = {
      s -> clusterRoutes(s)(c)
    }
    def clusterRoute(c: String): Call = {
      clusterRoutes(s)(c)
    }
    def clusterMenu(c: String): Menu = {
      Menu(s,IndexedSeq.empty,Some(clusterRoute(c)))
    }
    def clusterRouteBreadCrumb : BCDynamicLink = {
      BCDynamicLink( s, clusterRoutes(s))
    }
  }

  implicit class SchedulerRoute(s: String) {
    def schedulerRouteMenuItem(c: String): (String, Call) = {
      s -> schedulerRoutes(s)(c)
    }
    def schedulerRoute(c: String): Call = {
      schedulerRoutes(s)(c)
    }
    def schedulerMenu(c: String): Menu = {
      Menu(s,IndexedSeq.empty,Some(schedulerRoute(c)))
    }
    def schedulerRouteBreadCrumb : BCDynamicLink = {
      BCDynamicLink( s, schedulerRoutes(s))
    }
  }

  implicit class TopicRoute(s: String) {
    def topicRouteMenuItem(c: String, t: String): (String, Call) = {
      s -> topicRoutes(s)(c,t)
    }
    def topicRoute(c: String, t: String): Call = {
      topicRoutes(s)(c,t)
    }
  }
}
