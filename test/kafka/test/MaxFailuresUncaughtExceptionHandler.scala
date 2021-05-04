package kafka.test

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse._

import java.time.Instant
import java.time.temporal.ChronoUnit

class MaxFailuresUncaughtExceptionHandler(val maxFailures: Int, val maxTimeIntervalMillis: Long) extends StreamsUncaughtExceptionHandler {
  private var previousErrorTime: Instant = null
  private var currentFailureCount = 0

  override def handle(throwable: Throwable): StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse = {
    currentFailureCount += 1
    val currentErrorTime = Instant.now
    if (previousErrorTime == null) previousErrorTime = currentErrorTime
    val millisBetweenFailure = ChronoUnit.MILLIS.between(previousErrorTime, currentErrorTime)
    if (currentFailureCount >= maxFailures) if (millisBetweenFailure <= maxTimeIntervalMillis) return SHUTDOWN_APPLICATION
    else {
      currentFailureCount = 0
      previousErrorTime = null
    }
    REPLACE_THREAD
  }
}