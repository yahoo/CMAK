/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package models.navigation

import play.api.mvc.Call

/**
 * @author hiral
 */
object BreadCrumbs {

  sealed trait BreadCrumb
  final case class BCDynamicText(cn: String => String) extends BreadCrumb
  final case class BCDynamicNamedLink(cn: String => String, cl : String => Call) extends BreadCrumb
  final case class BCDynamicMultiNamedLink(cn: String => String, cl : (String, List[String]) => Call) extends BreadCrumb
  final case class BCDynamicMultiNamedLink2(cn: String => String, cl : (String, List[String], String) => Call) extends BreadCrumb
  final case class BCDynamicLink(s: String, cl : String => Call) extends BreadCrumb
  final case class BCStaticLink(s: String, c: Call) extends BreadCrumb

  sealed trait BreadCrumbRendered
  final case class BCLink(s: String, url: String) extends BreadCrumbRendered
  final case class BCText(s: String) extends BreadCrumbRendered
  final case class BCActive(s: String) extends BreadCrumbRendered

  import models.navigation.QuickRoutes._

  val baseBreadCrumbs: Map[String, IndexedSeq[BreadCrumb]] = Map(
    "Clusters" -> IndexedSeq.empty[BreadCrumb],
    "Add Cluster" -> IndexedSeq("Clusters".baseRouteBreadCrumb)
  )

  val clusterBreadCrumbs: Map[String, IndexedSeq[BreadCrumb]] = Map(
    "Unknown Cluster Operation" -> IndexedSeq("Clusters".baseRouteBreadCrumb),
    "Delete Cluster" -> IndexedSeq("Clusters".baseRouteBreadCrumb, BCDynamicText(identity)),
    "Disable Cluster" -> IndexedSeq("Clusters".baseRouteBreadCrumb, BCDynamicText(identity)),
    "Enable Cluster" -> IndexedSeq("Clusters".baseRouteBreadCrumb, BCDynamicText(identity)),
    "Update Cluster" -> IndexedSeq("Clusters".baseRouteBreadCrumb, BCDynamicText(identity)),
    "Summary" -> IndexedSeq("Clusters".baseRouteBreadCrumb,BCDynamicText(identity)),
    "Brokers" -> IndexedSeq("Clusters".baseRouteBreadCrumb,BCDynamicNamedLink(identity,"Summary".clusterRoute)),
    "Broker View" -> IndexedSeq(
      "Clusters".baseRouteBreadCrumb,
      BCDynamicNamedLink(identity,"Summary".clusterRoute),
      "Brokers".clusterRouteBreadCrumb),
    "Topics" -> IndexedSeq("Clusters".baseRouteBreadCrumb,BCDynamicNamedLink(identity,"Summary".clusterRoute)),
    "Consumers" -> IndexedSeq("Clusters".baseRouteBreadCrumb,BCDynamicNamedLink(identity,"Summary".clusterRoute)),
    "Create Topic" -> IndexedSeq(
      "Clusters".baseRouteBreadCrumb,
      BCDynamicNamedLink(identity,"Summary".clusterRoute),
      "Topics".clusterRouteBreadCrumb),
    "Topic View" -> IndexedSeq(
      "Clusters".baseRouteBreadCrumb,
      BCDynamicNamedLink(identity,"Summary".clusterRoute),
      "Topics".clusterRouteBreadCrumb),
    "Consumer View" -> IndexedSeq(
      "Clusters".baseRouteBreadCrumb,
      BCDynamicNamedLink(identity,"Summary".clusterRoute),
      "Consumers".clusterRouteBreadCrumb),
    "Consumed Topic View" -> IndexedSeq(
      "Clusters".baseRouteBreadCrumb,
      BCDynamicNamedLink(identity,"Summary".clusterRoute),
      "Consumers".clusterRouteBreadCrumb),
    "Logkafkas" -> IndexedSeq("Clusters".baseRouteBreadCrumb,BCDynamicNamedLink(identity,"Summary".clusterRoute)),
    "Create Logkafka" -> IndexedSeq(
      "Clusters".baseRouteBreadCrumb,
      BCDynamicNamedLink(identity,"Summary".clusterRoute),
      "Logkafkas".clusterRouteBreadCrumb),
    "Logkafka View" -> IndexedSeq(
      "Clusters".baseRouteBreadCrumb,
      BCDynamicNamedLink(identity,"Summary".clusterRoute),
      "Logkafkas".clusterRouteBreadCrumb),
    "Preferred Replica Election" -> IndexedSeq(
      "Clusters".baseRouteBreadCrumb,
      BCDynamicNamedLink(identity,"Summary".clusterRoute)),
    "Reassign Partitions" -> IndexedSeq(
      "Clusters".baseRouteBreadCrumb,
      BCDynamicNamedLink(identity,"Summary".clusterRoute)),
    "Run Election" -> IndexedSeq(
      "Clusters".baseRouteBreadCrumb,
      BCDynamicNamedLink(identity,"Summary".clusterRoute),
      "Preferred Replica Election".clusterRouteBreadCrumb
    )
  )

  val topicBreadCrumbs: Map[String, IndexedSeq[BreadCrumb]] = Map(
    "Topic View" -> IndexedSeq(
      "Clusters".baseRouteBreadCrumb,
      BCDynamicNamedLink(identity,"Summary".clusterRoute),
      "Topics".clusterRouteBreadCrumb,
      BCDynamicMultiNamedLink(identity,"Topic View".topicRoute)
    )
  )

  val consumerBreadCrumbs: Map[String, IndexedSeq[BreadCrumb]] = Map(
    "Consumer View" -> IndexedSeq(
      "Clusters".baseRouteBreadCrumb,
      BCDynamicNamedLink(identity,"Summary".clusterRoute),
      "Consumers".clusterRouteBreadCrumb,
      BCDynamicMultiNamedLink2(identity,"Consumer View".consumerRoute)
    )
  )
  val logkafkaBreadCrumbs: Map[String, IndexedSeq[BreadCrumb]] = Map(
    "Logkafka View" -> IndexedSeq(
      "Clusters".baseRouteBreadCrumb,
      BCDynamicNamedLink(identity,"Summary".clusterRoute),
      "Logkafkas".clusterRouteBreadCrumb,
      BCDynamicMultiNamedLink(identity,"Logkafka View".logkafkaRoute)
    )
  )

  def withView(s: String) : IndexedSeq[BreadCrumbRendered] = {
    val rendered : IndexedSeq[BreadCrumbRendered] = baseBreadCrumbs.getOrElse(s,IndexedSeq.empty[BreadCrumb]) map {
      case BCStaticLink(n,c) => BCLink(n,c.toString())
      case a: Any => throw new IllegalArgumentException(s"Only static link supported : $a")
    }
    rendered :+ BCActive(s)
  }

  private[this] def renderWithCluster(s: String, clusterName: String) : IndexedSeq[BreadCrumbRendered] = {
    clusterBreadCrumbs.getOrElse(s,IndexedSeq.empty[BreadCrumb]) map {
      case BCStaticLink(n,c) => BCLink(n,c.toString())
      case BCDynamicNamedLink(cn, cl) => BCLink(cn(clusterName),cl(clusterName).toString())
      case BCDynamicLink(cn, cl) => BCLink(cn,cl(clusterName).toString())
      case BCDynamicText(cn) => BCText(cn(clusterName))
      case _ => BCText("ERROR")
    }
  }

  def withNamedViewAndCluster(s: String, clusterName: String, name: String) : IndexedSeq[BreadCrumbRendered] = {
    renderWithCluster(s, clusterName) :+ BCActive(name)
  }

  def withViewAndCluster(s: String, clusterName: String) : IndexedSeq[BreadCrumbRendered] = {
    withNamedViewAndCluster(s, clusterName, s)
  }

  private[this] def renderWithClusterAndTopic(s: String, clusterName: String, topic: String) : IndexedSeq[BreadCrumbRendered] = {
    topicBreadCrumbs.getOrElse(s,IndexedSeq.empty[BreadCrumb]) map {
      case BCStaticLink(n,c) => BCLink(n,c.toString())
      case BCDynamicNamedLink(cn, cl) => BCLink(cn(clusterName),cl(clusterName).toString())
      case BCDynamicMultiNamedLink(cn, cl) => BCLink(cn(topic),cl(clusterName,List(topic)).toString())
      case BCDynamicLink(cn, cl) => BCLink(cn,cl(clusterName).toString())
      case BCDynamicText(cn) => BCText(cn(clusterName))
      case any => throw new UnsupportedOperationException(s"Unhandled breadcrumb : $any")
    }
  }

  private[this] def renderWithClusterAndConsumer(s: String, clusterName: String, consumer: String, consumerType: String, topic: String = "") : IndexedSeq[BreadCrumbRendered] = {
    consumerBreadCrumbs.getOrElse(s,IndexedSeq.empty[BreadCrumb]) map {
      case BCStaticLink(n,c) => BCLink(n,c.toString())
      case BCDynamicNamedLink(cn, cl) => BCLink(cn(clusterName),cl(clusterName).toString())
      case BCDynamicMultiNamedLink(cn, cl) => BCLink(cn(consumer),cl(clusterName,List(consumer)).toString())
      case BCDynamicLink(cn, cl) => BCLink(cn,cl(clusterName).toString())
      case BCDynamicText(cn) => BCText(cn(clusterName))
      case BCDynamicMultiNamedLink2(cn, cl) => BCLink(cn(consumer),cl(clusterName,List(consumer), consumerType).toString())
      case any => throw new UnsupportedOperationException(s"Unhandled breadcrumb : $any")
    }
  }

  def withNamedViewAndClusterAndTopic(s: String, clusterName: String, topic: String, name: String) : IndexedSeq[BreadCrumbRendered] = {
    renderWithClusterAndTopic(s, clusterName,topic) :+ BCActive(name)
  }

  private[this] def renderWithClusterAndLogkafka(s: String, clusterName: String, logkafka_id: String, log_path: String) : IndexedSeq[BreadCrumbRendered] = {
    val hl = logkafka_id + "?" + log_path
    logkafkaBreadCrumbs.getOrElse(s,IndexedSeq.empty[BreadCrumb]) map {
      case BCStaticLink(n,c) => BCLink(n,c.toString())
      case BCDynamicNamedLink(cn, cl) => BCLink(cn(clusterName),cl(clusterName).toString())
      case BCDynamicMultiNamedLink(cn, cl) => BCLink(cn(hl),cl(clusterName,List(logkafka_id, log_path)).toString())
      case BCDynamicLink(cn, cl) => BCLink(cn,cl(clusterName).toString())
      case BCDynamicText(cn) => BCText(cn(clusterName))
      case any => throw new UnsupportedOperationException(s"Unhandled breadcrumb : $any")
    }
  }

  def withNamedViewAndClusterAndLogkafka(s: String, clusterName: String, logkafka_id: String, log_path: String, name: String) : IndexedSeq[BreadCrumbRendered] = {
    renderWithClusterAndLogkafka(s, clusterName, logkafka_id, log_path) :+ BCActive(name)
  }
  def withNamedViewAndClusterAndConsumerWithType(s: String, clusterName: String, consumer: String, consumerType: String, name: String) : IndexedSeq[BreadCrumbRendered] = {
    renderWithClusterAndConsumer(s, clusterName, consumer, consumerType) :+ BCActive(name)
  }
}
