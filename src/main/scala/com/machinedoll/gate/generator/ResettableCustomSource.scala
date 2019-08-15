package com.machinedoll.gate.generator

import java.util.Calendar

import com.machinedoll.gate.schema.{DescriptionExample, EventTest}
import org.apache.flink.api.common.state.ListState
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction

class ResettableCustomSource extends SourceFunction[EventTest] with CheckpointedFunction{
  var isRunning: Boolean = true
  var cnt: Long = _
  var offsetState: ListState[EventTest] = _
  val calenderInstance = Calendar.getInstance()



//  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
//    while (isRunning && cnt < Long.MaxValue) {
//      ctx.getCheckpointLock.synchronized {
//        cnt += 1
//        ctx.collect(cnt)
//      }
//    }
//  }

//  override def cancel(): Unit = isRunning = false
//
//  override def snapshotState(context: FunctionSnapshotContext): Unit = {
//    offsetState.clear()
//    offsetState.add(cnt)
//  }
//
//  override def initializeState(context: FunctionInitializationContext): Unit = {
//    val desc = new ListStateDescriptor[Long]("offset", classOf[Long])
//    offsetState = context.getOperatorStateStore.getListState(desc)
//
//    val it = offsetState.get()
//    cnt = if (null == it || !it.iterator().hasNext) {
//      -1L
//    } else {
//      it.iterator().next()
//    }
//  }
  override def run(ctx: SourceFunction.SourceContext[EventTest]): Unit =
    while (isRunning && cnt < Long.MaxValue) {
      ctx.getCheckpointLock.synchronized{
        cnt += 1
        ctx.collect(new EventTest(id = cnt.toInt,
          timestamp = calenderInstance.getTimeInMillis,
          description = new DescriptionExample(0, "0")) )
      }
    }

  override def cancel(): Unit = isRunning = false

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    offsetState.clear()
    offsetState.add(
      new EventTest(cnt.toInt,
        context.getCheckpointTimestamp,
        description = new DescriptionExample(0, "0")))
  }

  override def initializeState(context: FunctionInitializationContext): Unit = ???
}
