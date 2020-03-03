/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package org.apache.kafka.common.metrics
import java.util

import grizzled.slf4j.Logging

/*
Override Kafka Client's implementation since we don't want to always report to Jmx
 */
class JmxReporter(prefix: String) extends MetricsReporter with Logging {
  override def init(metrics: util.List[KafkaMetric]): Unit = {}

  override def metricChange(metric: KafkaMetric): Unit = {}

  override def metricRemoval(metric: KafkaMetric): Unit = {}

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {
    info("Using cmak JmxReporter")
  }
}
