/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.base

import kafka.manager.model.ActorModel.{ActorRequest, QueryRequest}

/**
 * @author hiral
 */
abstract class BaseQueryActor extends BaseActor {

  final def processActorRequest(request: ActorRequest): Unit = {
    request match  {
      case queryRequest: QueryRequest =>
        processQueryRequest(queryRequest)
      case any: Any => log.warning("bqa : processActorRequest : Received unknown message: {}", any)
    }
  }

  def processQueryRequest(request: QueryRequest): Unit

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = super.preStart()

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = super.postStop()

  @scala.throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = super.preRestart(reason, message)
}
