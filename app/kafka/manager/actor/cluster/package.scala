/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.actor

import grizzled.slf4j.Logging
import kafka.manager.features.{ClusterFeatures, ClusterFeature}

import scala.util.{Failure, Try}

/**
 * Created by hiral on 12/1/15.
 */
package object cluster {
  implicit class TryLogErrorHelper[T](t: Try[T]) extends Logging {
    def logError(s: => String) : Try[T] = {
      t match {
        case Failure(e) =>
          error(s, e)
        case _ => //do nothing
      }
      t
    }
  }

  def featureGate[T](af: ClusterFeature)(fn: => Unit)(implicit features: ClusterFeatures) : Unit = {
    if(features.features(af)) {
      fn
    } else {
      //do nothing
    }
  }
  def featureGate[T](af: ClusterFeature, af2: ClusterFeature)(fn: => Unit)(implicit features: ClusterFeatures) : Unit = {
    if(features.features(af) && features.features(af2)) {
      fn
    } else {
      //do nothing
    }
  }
  def featureGate[T](af: ClusterFeature, af2: ClusterFeature, af3: ClusterFeature)(fn: => Unit)(implicit features: ClusterFeatures) : Unit = {
    if(features.features(af) && features.features(af2) && features.features(af3)) {
      fn
    } else {
      //do nothing
    }
  }
  def featureGateFold[T](af: ClusterFeature)(elseFn: => T, fn: => T)(implicit features: ClusterFeatures) : T = {
    if(features.features(af)) {
      fn
    } else {
      elseFn
    }
  }
}
