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

  val baseRoutes : Map[String, () => Call] = Map(
    "Clusters" -> { () => controllers.routes.Application.index },
    "List" -> { () => controllers.routes.Application.index },
    "Add Cluster" -> { () => controllers.routes.Cluster.addCluster }
  )
  val clusterRoutes : Map[String, String => Call] = Map(
    "Update Cluster" -> controllers.routes.Cluster.updateCluster,
    "Summary" -> controllers.routes.Cluster.cluster,
    "Brokers" -> controllers.routes.Cluster.brokers,
    "Topics" -> controllers.routes.Topic.topics,
    "Consumers" -> controllers.routes.Consumer.consumers,
    "List" -> controllers.routes.Topic.topics,
    "Create" -> controllers.routes.Topic.createTopic,
    "Preferred Replica Election" -> controllers.routes.PreferredReplicaElection.preferredReplicaElection,
    "Schedule Leader Election" -> controllers.routes.PreferredReplicaElection.scheduleRunElection,
    "Reassign Partitions" -> controllers.routes.ReassignPartitions.reassignPartitions,
    "Logkafkas" -> controllers.routes.Logkafka.logkafkas,
    "List Logkafka" -> controllers.routes.Logkafka.logkafkas,
    "Create Logkafka" -> controllers.routes.Logkafka.createLogkafka
  )
  val topicRoutes : Map[String, (String, String) => Call] = Map(
    "Topic View" -> ((c, t) => controllers.routes.Topic.topic(c, t, force=false)),
    "Add Partitions" -> controllers.routes.Topic.addPartitions,
    "Update Config" -> controllers.routes.Topic.addPartitions
  )
  val consumerRoutes : Map[String, (String, String, String) => Call] = Map(
    "Consumer View" -> controllers.routes.Consumer.consumer
  )
  val logkafkaRoutes : Map[String, (String, String, String) => Call] = Map(
    "Logkafka View" -> controllers.routes.Logkafka.logkafka,
    "Update Config" -> controllers.routes.Logkafka.updateConfig
  )

  implicit class BaseRoute(s: String) {
    def baseRouteMenuItem : (String, Call) = {
      s -> baseRoutes(s)()
    }
    def baseRoute : Call = {
      baseRoutes(s)()
    }
    def baseMenu(c: String): Menu = {
      Menu(s,IndexedSeq.empty,Some(baseRoute))
    }
    def baseRouteBreadCrumb : BCStaticLink = {
      BCStaticLink(s, baseRoutes(s)())
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

  implicit class TopicRoute(s: String) {
    def topicRouteMenuItem(c: String, t: String): (String, Call) = {
      s -> topicRoutes(s)(c,t)
    }
    def topicRoute(c: String, t: List[String]): Call = {
      topicRoutes(s)(c,t.head)
    }
  }

  implicit class ConsumerRoute(s: String) {
    def consumerRouteMenuItem(cluster: String, consumer: String, consumerType: String): (String, Call) = {
      s -> consumerRoutes(s)(cluster,consumer,consumerType)
    }
    def consumerRoute(cluster: String, consumer: List[String], consumerType: String): Call = {
      consumerRoutes(s)(cluster,consumer.head, consumerType)
    }
  }

  implicit class LogkafkaRoute(s: String) {
    def logkafkaRouteMenuItem(c: String, h: String, l:String): (String, Call) = {
      s -> logkafkaRoutes(s)(c,h,l)
    }
    def logkafkaRoute(c: String, hl: List[String]): Call = {
      val logkafka_id = hl(0)
      val log_path = hl(1)
      logkafkaRoutes(s)(c,logkafka_id,log_path)
    }
  }
}
