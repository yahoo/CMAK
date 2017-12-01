/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import java.nio.charset.StandardCharsets
import java.text.NumberFormat

/**
 * @author hiral
 */
package object utils {
  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  import org.json4s.jackson.Serialization.{read, write}
  implicit val formats = DefaultFormats
  private[this] val numberFormat = NumberFormat.getInstance()
  
  implicit class LongFormatted(val x: Long) {
    def formattedAsDecimal = numberFormat.format(x)  
  }

  implicit def serializeString(data: String) : Array[Byte] = {
    data.getBytes(StandardCharsets.UTF_8)
  }

  implicit def deserializeString(data: Array[Byte]) : String  = {
    new String(data, StandardCharsets.UTF_8)
  }

  def toJson(map: Map[String, Any]): String = {
    write(map)
  }
  
  def toJson(s: String) : String = {
    "\"" + s + "\""
  }

  def fromJson[T](s: String) : T = {
    read(s)
  }

  def parseJson(s: String) : JValue = {
    parse(s)
  }

  @throws[UtilException]
  def checkCondition(cond: Boolean, error: UtilError) : Unit = {
    if(!cond) {
      throw new UtilException(error)
    }
  }

  @throws[UtilException]
  def throwError [T] (error: UtilError) : T = {
    throw new UtilException(error)
  }
}
