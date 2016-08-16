package ru.wobot.fc

import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

@SerialVersionUID(1L)
class StringGeneratingSourceFunction (val numElements: Long) extends RichParallelSourceFunction[String] with CheckpointedAsynchronously[Integer] {
  private var index: Int = 0
  private var isRunning: Boolean = true

  @throws[Exception]
  def run(ctx: SourceFunction.SourceContext[String]) {
    val step: Int = getRuntimeContext.getNumberOfParallelSubtasks
    if (index == 0) index = getRuntimeContext.getIndexOfThisSubtask
    while (isRunning && index < numElements) {
      Thread.sleep(1)
      val id: Long = Thread.currentThread().getId
      this.synchronized {
      ctx.collect("message " + index)
      index += step
      }
    }
  }

  def cancel() {
    isRunning = false
  }

  def snapshotState(checkpointId: Long, checkpointTimestamp: Long): Integer = index

  def restoreState(state: Integer) {
    index = state
  }
}
