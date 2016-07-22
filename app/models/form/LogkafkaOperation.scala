/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package models.form

/**
 * @author hiral
 */

sealed trait LogkafkaOperation

case class LKConfig(name: String, value: Option[String])

case class CreateLogkafka(logkafka_id: String, log_path: String, configs: List[LKConfig]) extends LogkafkaOperation
case class DeleteLogkafka(logkafka_id: String, log_path: String) extends LogkafkaOperation
case class UpdateLogkafkaConfig(logkafka_id: String, log_path: String, configs: List[LKConfig]) extends LogkafkaOperation
case class UnknownLKO(op: String) extends LogkafkaOperation 
