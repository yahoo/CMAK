/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.utils

/**
 * @author hiral
 */
abstract class UtilError(msg: String) {
  override def toString : String = msg
}


case class UtilException(error: UtilError) extends IllegalArgumentException(error.toString)


