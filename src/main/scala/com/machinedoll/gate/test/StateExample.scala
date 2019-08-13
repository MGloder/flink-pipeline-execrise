package com.machinedoll.gate.test

import java.{lang, util}

import com.machinedoll.gate.generator.SimpleSensorReadingGenerator
import com.machinedoll.gate.schema.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.CheckpointListener
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.util.Collector

object StateExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(3000)

    val sensorData = env.addSource(new SimpleSensorReadingGenerator())

    val keyedSensorData = sensorData.keyBy(_.id)

    val tempDiff = keyedSensorData

      // flatMapWithState accept Tuple2
//      .flatMapWithState[(String, Float, Float), Float]{
//      case (in: SensorReading, None) => (List.empty, Some(in.reading))
//      case (in: SensorReading, lastRead: Some[Float]) => {
//        val tempDiff = (in.reading - lastRead.get).abs
//         > threshold
//        if (tempDiff > 10.0f) {
//          (List((in.id, in.reading, tempDiff)), Some(in.reading))
//        }else{
//          (List.empty, Some(in.reading))
//        }
//      }
//    }
      .flatMap(new TemperatureAlertFunction(10f))
      .uid("Example UID")
//      .flatMap(new ExampleListener())
      .print()

    env.execute("State Example")

  }

}

case class TemperatureFluctuation(id: String, temperature: Float, changes: Float)

class TemperatureAlertFunction(threshold: Float) extends RichFlatMapFunction[SensorReading, TemperatureFluctuation] {

  private var lastTempState: ValueState[Float] = _

  override def open(parameters: Configuration): Unit = {
    val lastTempDescriptor = new ValueStateDescriptor[Float]("lastTemp", classOf[Float])

    lastTempState = getRuntimeContext.getState[Float](lastTempDescriptor)
  }

  override def flatMap(in: SensorReading, collector: Collector[TemperatureFluctuation]): Unit = {
    val lastTempReading = lastTempState.value()

    val tempDiff = (in.reading - lastTempReading).abs

    if (tempDiff > threshold) {
      collector.collect(TemperatureFluctuation(in.id, in.reading, tempDiff))
    }

    this.lastTempState.update(in.reading)
  }
}
//class HighTemperatureCounter(threshold: Float)
//  extends RichFlatMapFunction[SensorReading, (Int, Long)]
//    with ListCheckpointed[java.lang.Long] {
//
//  private lazy val subtaskIdx = getRuntimeContext.getIndexOfThisSubtask
//
//  private var highCount = 0L
//
//  override def flatMap(value: SensorReading, out: Collector[(Int, Long)]): Unit = {
//    if (value.reading > threshold) {
//      highCount += 1
//      out.collect((subtaskIdx, highCount))
//    }
//  }
//
//  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[lang.Long] = {
//    java.util.Collections.singletonList(highCount)
//  }
//
//  override def restoreState(state: util.List[java.lang.Long]): Unit = {
//    highCount = 0
//    for (cnt <- state) {
//      highCount += cnt
//    }
//  }
//}

//class ExampleListener extends RichFlatMapFunction[SensorReading, (Int, Long)] with CheckpointListener {
//  override def flatMap(value: SensorReading, out: Collector[(Int, Long)]): Unit = {
//    out.collect((value.id.toInt, value.reading.toLong))
//  }
//
//  override def notifyCheckpointComplete(l: Long): Unit = {
//    println("checkpoint completed" + l)
//  }
//}
