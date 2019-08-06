package com.machinedoll.gate.test.join

import com.machinedoll.gate.generator.SimpleSensorReadingGenerator
import com.machinedoll.gate.schema.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
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
      .keyBy(_.id)
      .process(new ExampleProcessFunction)



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

  lazy val lastTemp: ValueState[Float] = getRuntimeContext.getState(
    new ValueStateDescriptor[Float]("lastTemp", Types.of[Float])
  )

  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("timer", Types.of[Long])
  )

  override def processElement(value: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                              out: Collector[String]): Unit = {
    val prevTemp = lastTemp.value()
    lastTemp.update(value.reading)

    val curTimerTimestamp = currentTimer.value()
    if (prevTemp == 0.0 || value.reading < prevTemp) {
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
      currentTimer.clear()
    } else if (value.reading > prevTemp && curTimerTimestamp == 0) {
      val timerTs = ctx.timerService().currentProcessingTime() + 1000
      ctx.timerService().registerEventTimeTimer(timerTs)
      currentTimer.update(timerTs)
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    out.collect("Temperature of sensor " + ctx.getCurrentKey + " monotonically increased for 1 second")
    currentTimer.clear()
  }
}