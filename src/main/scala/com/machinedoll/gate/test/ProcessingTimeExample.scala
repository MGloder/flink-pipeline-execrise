package com.machinedoll.gate.test

import com.machinedoll.gate.generator.SimpleSequenceObjectGenerator
import com.machinedoll.gate.schema.EventTest
import org.apache.commons.io.output.TaggedOutputStream
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time
object ProcessingTimeExample {
  def main(args: Array[String]) : Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val objectStream = env.
      addSource(new SimpleSequenceObjectGenerator(100, 1, 1, 2))

    objectStream.print()

    objectStream
      .map(x => (x.id, x.count))
      .keyBy(_._1)
      .timeWindow(Time.seconds(1))
//      .allowedLateness(Time.milliseconds(1000))
      .reduce((a, b) => (a._1, a._2 + b._2))
      .print()

    env.execute()
  }
}
