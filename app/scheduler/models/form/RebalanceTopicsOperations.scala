package scheduler.models.form

sealed trait RebalanceTopicsOperations

case class RebalanceTopics(ids: String, topics: Option[String]) extends RebalanceTopicsOperations


