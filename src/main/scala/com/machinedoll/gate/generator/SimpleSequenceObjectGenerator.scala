package com.machinedoll.gate.generator

import java.util.Calendar

import com.machinedoll.gate.schema.{DescriptionExample, EventTest}
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

class SimpleSequenceObjectGenerator(interval: Long, numEvent: Int, numDes: Int) extends RichParallelSourceFunction[EventTest] {

  var running: Boolean = true

  override def run(sourceContext: SourceContext[EventTest]): Unit = {
    val rand = new Random()

    val taskId = this.getRuntimeContext.getIndexOfThisSubtask
    val descriptionList: List[DescriptionExample] = (0 to numDes).toList.map {
      d =>
        new DescriptionExample(d, "This is a description msg for {} with ".formatted(d.toString))
    }

    val watermarkInterval = 2000 // 1s per watermark
    var lastWatermark = Calendar.getInstance().getTimeInMillis
    while (running) {
      val curTime = Calendar.getInstance.getTimeInMillis
      val sourceList = (0 to rand.nextInt(numEvent)).foreach{
        i => sourceContext.collect(EventTest(i, curTime, descriptionList(rand.nextInt(numDes))))
      }

      Thread.sleep(3000)

      sourceContext.collect(EventTest(0, curTime - 61 * 1000, new DescriptionExample(0, "Special Event")))

      Thread.sleep(interval)
    }
  }

  override def cancel(): Unit = running = false
}
