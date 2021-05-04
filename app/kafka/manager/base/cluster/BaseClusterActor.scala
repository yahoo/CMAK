/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.base.cluster

import kafka.manager.base.{BaseActor, BaseCommandActor, BaseQueryActor, BaseQueryCommandActor}
import kafka.manager.features.ClusterFeatures
import kafka.manager.model.{ClusterConfig, ClusterContext}

/**
 * Created by hiral on 12/1/15.
 */
trait BaseClusterActor {
  this : BaseActor =>

  protected implicit def clusterContext: ClusterContext
  protected implicit lazy val clusterConfig: ClusterConfig = clusterContext.config
  protected implicit lazy val clusterFeatures: ClusterFeatures = clusterContext.clusterFeatures
}

abstract class BaseClusterCommandActor extends BaseCommandActor with BaseClusterActor
abstract class BaseClusterQueryActor extends BaseQueryActor with BaseClusterActor
abstract class BaseClusterQueryCommandActor extends BaseQueryCommandActor with BaseClusterActor
