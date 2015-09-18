package scheduler.models.form

sealed trait BrokerOperations

case class Failover(failoverDelay: Option[String], failoverMaxDelay: Option[String], failoverMaxTries:Option[Int])

case class AddBroker(id: Int, cpus: Option[Double], mem: Option[Long], heap: Option[Long], port: Option[String],
                     bindAddress: Option[String], constraints: Option[String], options: Option[String],
                     log4jOptions: Option[String], jvmOptions: Option[String], stickinessPeriod: Option[String],
                     failover: Failover)

case class UpdateBroker(id: Int, cpus: Option[Double], mem: Option[Long], heap: Option[Long], port: Option[String],
                     bindAddress: Option[String], constraints: Option[String], options: Option[String],
                     log4jOptions: Option[String], jvmOptions: Option[String], stickinessPeriod: Option[String],
                     failover: Failover)
