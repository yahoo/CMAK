/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.base

import akka.actor.{Actor, ActorLogging}
import kafka.manager.model.ActorModel.{ActorErrorResponse, ActorRequest, ActorResponse}

/**
 * @author hiral
 */
abstract class BaseActor extends Actor with ActorLogging {

  var shutdown : Boolean = false

  final override def receive: Receive =  {
    case any: Any if shutdown =>
      val msg = s"Actor already shutdown, ignoring message $any"
      log.error(msg)
      sender ! ActorErrorResponse(msg)
    case request: ActorRequest =>
      processActorRequest(request)
    case response: ActorResponse=>
      processActorResponse(response)
    case any: Any => log.warning(s"ba : Received unknown message: ${any.getClass.getSimpleName}")
  }

  def processActorRequest(request: ActorRequest): Unit

  def processActorResponse(response: ActorResponse): Unit

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = super.preStart()

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = super.postStop()

  @scala.throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = super.preRestart(reason, message)
}
