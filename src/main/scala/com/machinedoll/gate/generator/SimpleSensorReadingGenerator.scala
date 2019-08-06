package com.machinedoll.gate.generator

import java.util.Calendar

import com.machinedoll.gate.schema.SensorReading
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

class SimpleSensorReadingGenerator extends RichParallelSourceFunction[SensorReading] {
  var running: Boolean = true

  val maxItem = 10
  val scaler = 100

  override def run(sourceContext: SourceContext[SensorReading]): Unit = {
    val rand = new Random()

    val taskId = this.getRuntimeContext.getIndexOfThisSubtask

    while (running) {
      val curTime = Calendar.getInstance.getTimeInMillis
      val sourceList = (0 to rand.nextInt(maxItem)).foreach{
        i => sourceContext.collect(SensorReading(i.toString, rand.nextFloat() * scaler, curTime))
      }

      Thread.sleep(3000)
    }
  }

  override def cancel(): Unit = running = false
}
