package kafka.manager.utils

import scala.collection.immutable.Queue

class FiniteQueue[A](q: Queue[A]) {

  def enqueueFinite[B >: A](elem: B, maxSize: Int): Queue[B] = {
    var ret = q.enqueue(elem)
    while (ret.size > maxSize) {
      ret = ret.dequeue._2
    }
    ret
  }
}