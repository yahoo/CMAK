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


import scala.util.matching.Regex
import org.slf4j.LoggerFactory

/**
 * Borrowed from kafka 0.8.1.1
 * https://git-wip-us.apache.org/repos/asf?p=kafka.git;a=blob;f=core/src/main/scala/kafka/common/Logkafka.scala
 */
object Logkafka {
  import kafka.manager.utils.LogkafkaErrors._

  val legalChars = "[a-zA-Z0-9\\._\\-]"
  val validHostnameRegex = "^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])$";
  val maxNameLength = 255
  val illegalPathChars = "[\\?*:|\"<>]"
  val maxPathLength = 255
  private val rgx = new Regex(legalChars + "+")
  private val illRgxPath = new Regex(illegalPathChars)
  lazy val logger = LoggerFactory.getLogger(this.getClass)

  def validateHostname(hostname: String) {
    checkCondition(hostname.length > 0, HostnameEmpty)
    checkCondition(hostname.length <= maxNameLength, InvalidHostnameLength)
    rgx.findFirstIn(hostname) match {
      case Some(t) =>
        checkCondition(t.equals(hostname), IllegalCharacterInName(hostname))
      case None =>
        checkCondition(false, IllegalCharacterInName(hostname))
    }
    checkCondition(hostname.matches(validHostnameRegex), InvalidHostname)
  }

  def validatePath(log_path: String) {
    checkCondition(log_path.length > 0, LogPathEmpty)
    checkCondition(log_path.startsWith("/"), LogPathNotAbsolute)
    checkCondition(log_path.length <= maxPathLength, InvalidLogPathLength)
    illRgxPath.findFirstIn(log_path) match {
      case Some(t) =>
        checkCondition(false, IllegalCharacterInName(log_path))
      case None =>
        checkCondition(true, IllegalCharacterInName(log_path))
    }
  }

  def parseJsonStr(hostname: String, jsonStr: String): Map[String, Map[String, String]] = {
    import org.json4s.jackson.JsonMethods._
    import org.json4s.scalaz.JsonScalaz._
    import scala.language.reflectiveCalls
    import org.json4s.JsonAST._
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
          logger.error(s"[hostname=${hostname}] Failed to parse logkafka hostname config : ${jsonStr}",e)
          Map.empty
    }
  }
}

object LogkafkaErrors {
  class HostnameEmpty private[LogkafkaErrors] extends UtilError("hostname is illegal, can't be empty")
  class LogPathEmpty private[LogkafkaErrors] extends UtilError("log path is illegal, can't be empty")
  class LogPathNotAbsolute private[LogkafkaErrors] extends UtilError("log path is illegal, must be absolute")
  class InvalidHostname private[LogkafkaErrors] extends UtilError(s"hostname is illegal, does not match regex ${Logkafka.validHostnameRegex}")
  class InvalidHostnameLength private[LogkafkaErrors] extends UtilError(
    "hostname is illegal, can't be longer than " + Logkafka.maxNameLength + " characters")
  class InvalidLogPathLength private[LogkafkaErrors] extends UtilError(
    "log path is illegal, can't be longer than " + Logkafka.maxPathLength + " characters")
  class IllegalCharacterInName private[LogkafkaErrors] (hostname: String) extends UtilError(
    "hostname " + hostname + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'")
  class IllegalCharacterInPath private[LogkafkaErrors] (log_path: String) extends UtilError(
    "log path " + log_path + " is illegal, contains a character in " + Logkafka.illegalPathChars)
  class HostnameNotExists private[LogkafkaErrors] (hostname: String) extends UtilError(s"Hostname not exists : $hostname")

  val HostnameEmpty = new HostnameEmpty
  val LogPathEmpty = new LogPathEmpty 
  val LogPathNotAbsolute = new LogPathNotAbsolute
  val InvalidHostname = new InvalidHostname
  val InvalidHostnameLength = new InvalidHostnameLength
  val InvalidLogPathLength = new InvalidLogPathLength
  def IllegalCharacterInName(hostname: String) = new IllegalCharacterInName(hostname)
  def IllegalCharacterInPath(log_path: String) = new IllegalCharacterInPath(log_path)
  def HostnameNotExists(hostname: String) = new HostnameNotExists(hostname)
}

