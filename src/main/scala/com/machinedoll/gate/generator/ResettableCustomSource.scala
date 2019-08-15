package com.machinedoll.gate.generator

import java.util.Calendar

import com.machinedoll.gate.schema.{DescriptionExample, EventTest}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction

class ResettableCustomSource extends SourceFunction[EventTest] with CheckpointedFunction{
  var isRunning: Boolean = true
  var cnt: Long = _
  var event: EventTest = _
  var offsetState: ListState[EventTest] = _
  val calenderInstance = Calendar.getInstance()

  override def run(ctx: SourceFunction.SourceContext[EventTest]): Unit =
    while (isRunning && cnt < Long.MaxValue) {
      ctx.getCheckpointLock.synchronized{
        cnt += 1
        event = new EventTest(id = cnt.toInt,
          timestamp = calenderInstance.getTimeInMillis,
          description = new DescriptionExample(0, "0"))
        ctx.collect(event)
      }
    }

  override def cancel(): Unit = isRunning = false

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    offsetState.clear()
    offsetState.add(event)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val desc = new ListStateDescriptor[EventTest]("offset", classOf[EventTest])
    offsetState = context.getOperatorStateStore.getListState(desc)

    val it = offsetState.get()
    event = if (null == it || !it.iterator().hasNext) {
      new EventTest(-1,
        calenderInstance.getTimeInMillis,
        new DescriptionExample(-1, "-1"))
    } else {
      it.iterator().next()
    }
  }
}
