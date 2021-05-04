/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager.utils

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.ClassTag

/**
 * @author hiral
 */
trait CuratorAwareTest extends AnyFunSuite with BeforeAndAfterAll with ZookeeperServerAwareTest {

  private[this] var curator: Option[CuratorFramework] = None

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    val curatorFramework = CuratorFrameworkFactory.newClient(testServer.getConnectString, retryPolicy)
    curatorFramework.start
    curator = Some(curatorFramework)
  }

  override protected def afterAll(): Unit = {
    curator.foreach(_.close())
    super.afterAll()
  }

  protected def withCurator(fn: CuratorFramework => Unit): Unit = {
    curator.foreach(fn)
  }

  protected def produceWithCurator[T](fn: CuratorFramework => T) : T = {
    require(curator.isDefined,"Cannot produce with no curator defined!")
    fn(curator.get)
  }

  protected def checkError[T](fn: => Any)(implicit tag: ClassTag[T]): Unit = {
    try {
      fn
      throw new RuntimeException(s"expected ${tag.runtimeClass} , but no exceptions were thrown!")
    } catch {
      case UtilException(caught) =>
        if(!tag.runtimeClass.isAssignableFrom(caught.getClass)) {
          throw new RuntimeException(s"expected ${tag.runtimeClass} , found ${caught.getClass}, value=$caught")
        }
      case throwable: Throwable =>
        throw new RuntimeException(s"expected ${tag.runtimeClass} , found ${throwable.getClass}", throwable)
    }
  }

}
