/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package models.navigation

/**
 * @author hiral
 */
object Menus {
  import models.navigation.QuickRoutes._
  def clusterMenus(cluster: String) : IndexedSeq[Menu] = IndexedSeq(
    Menu("Cluster",IndexedSeq(
      "Summary".clusterRouteMenuItem(cluster),
      "List".baseRouteMenuItem,
      "Add Cluster".baseRouteMenuItem),
      None),
    "Brokers".clusterMenu(cluster),
    Menu("Topic",IndexedSeq(
      "List".clusterRouteMenuItem(cluster),
      "Create".clusterRouteMenuItem(cluster)),
      None),
    "Preferred Replica Election".clusterMenu(cluster),
    "Reassign Partitions".clusterMenu(cluster)
  )

  def schedulerMenus(scheduler: String) : IndexedSeq[Menu] = IndexedSeq(
    Menu("Scheduler",IndexedSeq(
      "Summary".schedulerRouteMenuItem(scheduler),
      "List".baseRouteMenuItem,
      "Add Scheduler".baseRouteMenuItem),
      None),
    Menu("Brokers",IndexedSeq(
      "Add Broker".schedulerRouteMenuItem(scheduler)),
      None),
    "Rebalance Topics".schedulerMenu(scheduler)
  )

  def indexMenu : IndexedSeq[Menu] = IndexedSeq(
    Menu("Cluster",IndexedSeq(
      "List".baseRouteMenuItem,
      "Add Cluster".baseRouteMenuItem),
      None),

      Menu("Scheduler",IndexedSeq(
      "List".baseRouteMenuItem,
      "Add Scheduler".baseRouteMenuItem),
      None)
  )
}
