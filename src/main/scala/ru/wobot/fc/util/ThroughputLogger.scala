package ru.wobot.fc.util

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

object ThroughputLogger {
  private val LOG: Logger = LoggerFactory.getLogger(ThroughputLogger.getClass)
}

class ThroughputLogger[T](var elementSize: Int, var logfreq: Long) extends FlatMapFunction[T, Integer] {
  private var totalReceived: Long = 0
  private var lastTotalReceived: Long = 0
  private var lastLogTimeMs: Long = -1

  @throws[Exception]
  def flatMap(element: T, collector: Collector[Integer]) {
    totalReceived += 1
    if (totalReceived % logfreq == 0) {
      // throughput over entire time
      val now: Long = System.currentTimeMillis
      // throughput for the last "logfreq" elements
      if (lastLogTimeMs == -1) {
        // init (the first)
        lastLogTimeMs = now
        lastTotalReceived = totalReceived
      }
      else {
        val timeDiff: Long = now - lastLogTimeMs
        val elementDiff: Long = totalReceived - lastTotalReceived
        val ex: Double = 1000 / timeDiff.toDouble
        ThroughputLogger.LOG.info(s"During the last $timeDiff ms, we received $elementDiff elements. That's ${elementDiff * ex} elements/second/core. ${elementDiff * ex * elementSize / 1024 / 1024} MB/sec/core. GB received ${(totalReceived * elementSize) / 1024 / 1024 / 1024}")
        // reinit
        lastLogTimeMs = now
        lastTotalReceived = totalReceived
      }
    }
  }
}