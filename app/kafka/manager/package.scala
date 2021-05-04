/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka

import java.nio.charset.StandardCharsets

/**
 * @author hiral
 */
package object manager {

  def nodeFromPath(s: String) : String = {
    val l = s.lastIndexOf("/")
    s.substring(l+1)
  }

  def asString(ba: Array[Byte]) : String = {
    new String(ba, StandardCharsets.UTF_8)
  }

  def asByteArray(str: String) : Array[Byte] = {
    str.getBytes(StandardCharsets.UTF_8)
  }

  implicit class PropertiesHelper(p: java.util.Properties) {
    def asMap: java.util.Map[_, _] = p.asInstanceOf[java.util.Map[_, _]]
  }
}
