package kafka.manager.utils

import play.twirl.api._

object repeatWithIndex {

  import play.api.data.Field

  def apply(field: play.api.data.Field, min: Int = 1)(f: (Field,Int) => Html) = {
    (0 until math.max(if (field.indexes.isEmpty) 0 else field.indexes.max + 1, min)).map(i => f(field("[" + i + "]"),i))
  }
}
