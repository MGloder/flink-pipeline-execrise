package com.machinedoll.gate.test.join

import com.machinedoll.gate.generator.{SimpleSensorReadingGenerator, SimpleSequenceObjectGenerator}
import com.machinedoll.gate.schema.{EventTest, SensorReading}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

object WatermarkExample {
  def main(args: Array[String]) : Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    // change 200 milliseconds to 5000 millisecond
//    env.getConfig.setAutoWatermarkInterval(5000)

    val sourceWithWatermark = env
      .addSource(new SimpleSensorReadingGenerator)
      .assignTimestampsAndWatermarks(new WatermarkAssigner())
//      .keyBy(_.id)
//      .process(new ExampleProcessFunction)



    sourceWithWatermark.print()

    env.execute("Watermark Example")
  }
}

class WatermarkAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{
  val bound: Long = 1000
  var maxTs: Long = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp)
    element.timestamp
  }
}

class ExampleProcessFunction() extends KeyedProcessFunction[String, SensorReading, String] {
  override def processElement(value: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                              out: Collector[String]): Unit = ???

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = super.onTimer(timestamp, ctx, out)
}