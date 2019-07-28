package com.machinedoll.gate.generator

import java.util.Calendar

import com.machinedoll.gate.schema.{DescriptionExample, EventTest}
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
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

    while (running) {
      val curTime = Calendar.getInstance.getTimeInMillis
      val sourceList = (0 to rand.nextInt(numEvent)).map {
        i =>
          sourceContext
            .collect(
              new EventTest(i, curTime, descriptionList(rand.nextInt(numDes)))
            )
      }
      Thread.sleep(interval)
    }
  }

  override def cancel(): Unit = running = false
}
