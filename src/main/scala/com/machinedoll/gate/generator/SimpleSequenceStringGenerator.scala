package com.machinedoll.gate.generator

import java.util.Calendar

import com.machinedoll.gate.schema.{DescriptionExample, EventTest}
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

class SimpleSequenceStringGenerator(interval: Long, numEvent: Int) extends RichParallelSourceFunction[String] {

  var running: Boolean = true

  override def run(sourceContext: SourceContext[String]): Unit = {
    val rand = new Random()

    val taskId = this.getRuntimeContext.getIndexOfThisSubtask

    val watermarkInterval = 2000 // 1s per watermark
    var lastWatermark = Calendar.getInstance().getTimeInMillis
    while (running) {
      val curTime = Calendar.getInstance.getTimeInMillis
      val sourceList = (0 to rand.nextInt(numEvent)).foreach{
        i => sourceContext.collect((curTime + i) .toString)
      }
      Thread.sleep(interval)
    }
  }

  override def cancel(): Unit = running = false
}
