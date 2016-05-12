/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager

import org.json4s._
import org.json4s.scalaz.JsonScalaz._

package object model {
  def fieldExtended[A: JSONR](name: String)(json: JValue): Result[A] = {
    val result = field[A](name)(json)
    result.leftMap {
      nel =>
        nel.map {
          case UnexpectedJSONError(was, expected) =>
            UncategorizedError(name, s"unexpected value : $was expected : ${expected.getSimpleName}", List.empty)
          case a => a
        }
    }
  }
}
