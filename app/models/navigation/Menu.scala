/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package models.navigation

import play.api.mvc.Call

/**
 * @author hiral
 */
case class Menu(title: String, items:Seq[(String,Call)], route : Option[Call]) {
  require(items.nonEmpty && route.isEmpty || items.isEmpty && route.isDefined, "Cannot have both menu items and a root link")
}
