package com.machinedoll.gate.generator

import java.util.Calendar

import com.machinedoll.gate.schema.{DescriptionExample, EventTest}
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

class SimpleSequenceObjectGenerator(interval: Long) extends RichParallelSourceFunction[EventTest] {

  var running: Boolean = true

  override def run(sourceContext: SourceContext[EventTest]): Unit = {
    val rand = new Random()

    val taskId = this.getRuntimeContext.getIndexOfThisSubtask
    val descriptionList: List[DescriptionExample] = (1 to 1000).toList.map {
      d =>
        new DescriptionExample(rand.nextInt(), "This is a description msg for {} with ".formatted(d.toString))
    }

    while (running) {
      val curTime = Calendar.getInstance.getTimeInMillis
      val sourceList = (1 to rand.nextInt(100)).map {
        i =>
          sourceContext
            .collect(
              new EventTest(i, curTime, descriptionList(rand.nextInt(100)))
            )
      }
      Thread.sleep(interval)
    }
  }

  override def cancel(): Unit = running = false
}
