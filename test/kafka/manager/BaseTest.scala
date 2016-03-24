package kafka.manager

import kafka.manager.model.ClusterTuning

/**
  * Created by hiral on 3/19/16.
  */
trait BaseTest {
  val defaultPoolSize = 2
  val defaultPoolQueueSize = 100
  val defaultPollingSeconds = 10
  val defaultTuning : ClusterTuning = ClusterTuning(
    Option(defaultPollingSeconds)
    ,Option(defaultPoolSize)
    ,Option(defaultPoolQueueSize)
    ,Option(defaultPoolSize)
    ,Option(defaultPoolQueueSize)
    ,Option(defaultPoolSize)
    ,Option(defaultPoolQueueSize)
    ,Option(defaultPollingSeconds)
    ,Option(defaultPollingSeconds)
    ,Option(defaultPoolSize)
    ,Option(defaultPoolQueueSize)
    ,Option(defaultPoolSize)
    ,Option(defaultPoolQueueSize)
    ,Option(defaultPoolSize)
    ,Option(defaultPoolQueueSize)
  )

  def getClusterTuning(defaultPoolSize: Int, defaultPoolQueueSize: Int, defaultPollingSeconds: Int) : ClusterTuning = {
    ClusterTuning(
      Option(defaultPollingSeconds)
      ,Option(defaultPoolSize)
      ,Option(defaultPoolQueueSize)
      ,Option(defaultPoolSize)
      ,Option(defaultPoolQueueSize)
      ,Option(defaultPoolSize)
      ,Option(defaultPoolQueueSize)
      ,Option(defaultPollingSeconds)
      ,Option(defaultPollingSeconds)
      ,Option(defaultPoolSize)
      ,Option(defaultPoolQueueSize)
      ,Option(defaultPoolSize)
      ,Option(defaultPoolQueueSize)
      ,Option(defaultPoolSize)
      ,Option(defaultPoolQueueSize)
    )
  }
}
