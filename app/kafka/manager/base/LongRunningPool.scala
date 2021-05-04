/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.base

import akka.pattern._

import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Try

/**
 * Created by hiral on 5/3/15.
 */

case class LongRunningPoolConfig(threadPoolSize: Int, maxQueueSize: Int)
trait LongRunningPoolActor extends BaseActor {
  
  protected val longRunningExecutor = new ThreadPoolExecutor(
    longRunningPoolConfig.threadPoolSize, longRunningPoolConfig.threadPoolSize,0L,TimeUnit.MILLISECONDS,new LinkedBlockingQueue[Runnable](longRunningPoolConfig.maxQueueSize))
  protected val longRunningExecutionContext = ExecutionContext.fromExecutor(longRunningExecutor)

  protected def longRunningPoolConfig: LongRunningPoolConfig
  
  protected def longRunningQueueFull(): Unit
  
  protected def hasCapacityFor(taskCount: Int): Boolean = {
    longRunningExecutor.getQueue.remainingCapacity() >= taskCount
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info("Shutting down long running executor...")
    Try(longRunningExecutor.shutdown())
    super.postStop()
  }

  protected def longRunning[T](fn: => Future[T])(implicit ec: ExecutionContext, ct: ClassTag[T]) : Unit = {
    if(longRunningExecutor.getQueue.remainingCapacity() == 0) {
      longRunningQueueFull()
    } else {
      fn match {
        case _ if ct.runtimeClass == classOf[Unit] =>
        //do nothing with unit
        case f =>
          f pipeTo sender
      }
    }
  }
}
