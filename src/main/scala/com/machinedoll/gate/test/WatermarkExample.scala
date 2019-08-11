package com.machinedoll.gate.test

import com.machinedoll.gate.generator.SimpleSequenceObjectGenerator
import com.machinedoll.gate.schema.EventTest
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

object WatermarkExample {
  def main(args: Array[String]) : Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(3000)

    val sourceWithWatermark = env
      .addSource(new SimpleSequenceObjectGenerator(2000, 1, 1, 1))
      .assignTimestampsAndWatermarks(new WatermarkAssigner())



//    sourceWithWatermark.getSideOutput().print()
//    sourceWithWatermark.print()

    env.execute("Watermark Example")
  }
}

class WatermarkAssigner() extends AssignerWithPeriodicWatermarks[EventTest]{
  val bound: Long = 60 * 1000
  var maxTs: Long = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    println("Generating water mark for " + maxTs)
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(element: EventTest, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp)
    element.timestamp
  }
}
