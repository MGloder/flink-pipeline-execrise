package com.machinedoll.gate.test

import com.machinedoll.gate.generator.SimpleSensorReadingGenerator
import com.machinedoll.gate.schema.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object WatermarkExample {
  def main(args: Array[String]) : Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    // change 200 milliseconds to 5000 millisecond
//    env.getConfig.setAutoWatermarkInterval(5000)

    val readings = env
      .addSource(new SimpleSensorReadingGenerator)
      .assignTimestampsAndWatermarks(new WatermarkAssigner())
      .keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(1)))
      .process(new ProcessWindowFunctionExample())
//      .process(new ExampleProcessFunction)
//    val monitoredReading: DataStream[SensorReading] = readings
//        .keyBy(_.id)
//        .process(new ExampleProcessFunction)
//        .process(new FreezingMonitor)

//    monitoredReading
//      .getSideOutput(new OutputTag[String]("freezing-alarms"))
//      .print()
//
//    readings
//      .keyBy(_.id)
      .print()

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

class FreezingMonitor() extends ProcessFunction[SensorReading, SensorReading] {

  lazy val freezingAlarmOutput: OutputTag[String] = new OutputTag[String]("freezing-alarms")

  override def processElement(value: SensorReading,
                              ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                              out: Collector[SensorReading]): Unit = {
    if (value.reading < 32.0) {
      ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${value.id}")
    }

    out.collect(value)
  }
}

//class ProcessWindowFunctionExample() extends ProcessWindowFunction[] {
//
//}