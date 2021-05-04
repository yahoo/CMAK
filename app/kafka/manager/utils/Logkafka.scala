/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.manager.utils


import grizzled.slf4j.Logging

import scala.util.matching.Regex

/**
 * Borrowed from kafka 0.8.1.1
 * https://git-wip-us.apache.org/repos/asf?p=kafka.git;a=blob;f=core/src/main/scala/kafka/common/Logkafka.scala
 */
object Logkafka extends Logging {
  import kafka.manager.utils.LogkafkaErrors._

  val legalChars = "[a-zA-Z0-9\\._\\-]"
  val validHostnameRegex = "^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])$"; // RFC 1123
  val maxNameLength = 255
  val illegalPathChars = "[\\?*:|\"<>]"
  val maxPathLength = 255
  private val rgx = new Regex(legalChars + "+")
  private val illRgxPath = new Regex(illegalPathChars)

  def validateHostname(hostname: String) {
    checkCondition(hostname.length > 0, HostnameEmpty)
    checkCondition(hostname.length <= maxNameLength, InvalidHostnameLength)
    rgx.findFirstIn(hostname) match {
      case Some(t) =>
        checkCondition(t.equals(hostname), IllegalCharacterInHostname(hostname))
      case None =>
        checkCondition(false, IllegalCharacterInHostname(hostname))
    }
    checkCondition(!hostname.matches("^localhost$"), HostnameIsLocalhost)
    checkCondition(hostname.matches(validHostnameRegex), InvalidHostname)
  }

  def validateLogkafkaId(logkafka_id: String) {
    checkCondition(logkafka_id.length > 0, LogkafkaIdEmpty)
    checkCondition(!(logkafka_id.equals(".") || logkafka_id.equals("..")), InvalidLogkafkaId)
    checkCondition(logkafka_id.length <= maxNameLength, InvalidLogkafkaIdLength)
    rgx.findFirstIn(logkafka_id) match {
      case Some(t) =>
        checkCondition(t.equals(logkafka_id), IllegalCharacterInLogkafkaId(logkafka_id))
      case None =>
        checkCondition(false, IllegalCharacterInLogkafkaId(logkafka_id))
    }
  }

  def validatePath(log_path: String) {
    checkCondition(log_path.length > 0, LogPathEmpty)
    checkCondition(log_path.startsWith("/"), LogPathNotAbsolute)
    checkCondition(log_path.length <= maxPathLength, InvalidLogPathLength)
    illRgxPath.findFirstIn(log_path) match {
      case Some(t) =>
        checkCondition(false, IllegalCharacterInPath(log_path))
      case None =>
        checkCondition(true, IllegalCharacterInPath(log_path))
    }

    val f = new java.io.File(log_path);
    val valid = try {
      f.getCanonicalPath()
      true
    } catch {
      case e: Exception => false
    }
    checkCondition(valid, InvalidLogPath)
  }

  def parseJsonStr(logkafka_id: String, jsonStr: String): Map[String, Map[String, String]] = {
    import org.json4s.JsonAST._
    import org.json4s.jackson.JsonMethods._

    import scala.language.reflectiveCalls
    try {
      implicit val formats = org.json4s.DefaultFormats
      val json = parse(jsonStr)
      val mapMutable: collection.mutable.Map[String, Map[String,String]] = collection.mutable.Map.empty
      for (JObject(list) <- json) {
          for ((log_path, JObject(c)) <- list) {
            mapMutable += log_path -> ((for {(k, JString(v)) <- c} yield (k -> v)).toMap)
          }
      }
      collection.immutable.Map(mapMutable.toList: _*)
    } catch {
      case e: Exception =>
          logger.error(s"[logkafka_id=${logkafka_id}] Failed to parse logkafka logkafka_id config : ${jsonStr}",e)
          Map.empty
    }
  }
}

object LogkafkaErrors {
  class HostnameEmpty private[LogkafkaErrors] extends UtilError("hostname is illegal, can't be empty")
  class HostnameIsLocalhost private[LogkafkaErrors] extends UtilError("hostname is illegal, can't be localhost")
  class LogkafkaIdEmpty private[LogkafkaErrors] extends UtilError("logkafka id is illegal, can't be empty")
  class LogPathEmpty private[LogkafkaErrors] extends UtilError("log path is illegal, can't be empty")
  class LogPathNotAbsolute private[LogkafkaErrors] extends UtilError("log path is illegal, must be absolute")
  class InvalidHostname private[LogkafkaErrors] extends UtilError(s"hostname is illegal, does not match regex ${Logkafka.validHostnameRegex}, which conforms to RFC 1123")
  class InvalidHostnameLength private[LogkafkaErrors] extends UtilError(
    "hostname is illegal, can't be longer than " + Logkafka.maxNameLength + " characters")
  class InvalidLogkafkaId private[LogkafkaErrors] extends UtilError("logkafka id is illegal, cannot be \".\" or \"..\"")
  class InvalidLogkafkaIdLength private[LogkafkaErrors] extends UtilError(
    "logkafka id is illegal, can't be longer than " + Logkafka.maxNameLength + " characters")
  class InvalidLogPath private[LogkafkaErrors] extends UtilError(s"log path is illegal")
  class InvalidLogPathLength private[LogkafkaErrors] extends UtilError(
    "log path is illegal, can't be longer than " + Logkafka.maxPathLength + " characters")
  class IllegalCharacterInHostname private[LogkafkaErrors] (hostname: String) extends UtilError(
    "hostname " + hostname + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'")
  class IllegalCharacterInLogkafkaId private[LogkafkaErrors] (logkafka_id: String) extends UtilError(
    "logkafka id " + logkafka_id + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'")
  class IllegalCharacterInPath private[LogkafkaErrors] (log_path: String) extends UtilError(
    "log path " + log_path + " is illegal, contains a character in " + Logkafka.illegalPathChars)
  class HostnameNotExists private[LogkafkaErrors] (hostname: String) extends UtilError(s"Hostname not exists : $hostname")
  class LogkafkaIdNotExists private[LogkafkaErrors] (logkafka_id: String) extends UtilError(s"LogkafkaId not exists : $logkafka_id")

  val HostnameEmpty = new HostnameEmpty
  val HostnameIsLocalhost = new HostnameIsLocalhost
  val LogkafkaIdEmpty = new LogkafkaIdEmpty
  val LogPathEmpty = new LogPathEmpty 
  val LogPathNotAbsolute = new LogPathNotAbsolute
  val InvalidHostname = new InvalidHostname
  val InvalidHostnameLength = new InvalidHostnameLength
  val InvalidLogkafkaId = new InvalidLogkafkaId
  val InvalidLogkafkaIdLength = new InvalidLogkafkaIdLength
  val InvalidLogPath = new InvalidLogPath
  val InvalidLogPathLength = new InvalidLogPathLength
  def IllegalCharacterInHostname(hostname: String) = new IllegalCharacterInHostname(hostname)
  def IllegalCharacterInLogkafkaId(logkafka_id: String) = new IllegalCharacterInLogkafkaId(logkafka_id)
  def IllegalCharacterInPath(log_path: String) = new IllegalCharacterInPath(log_path)
  def HostnameNotExists(hostname: String) = new HostnameNotExists(hostname)
  def LogkafkaIdNotExists(logkafka_id: String) = new LogkafkaIdNotExists(logkafka_id)
}

