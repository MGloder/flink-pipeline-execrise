package com.machinedoll.gate.test

import com.machinedoll.gate.generator.ResettableCustomSource
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CustomSourceSinkExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(100)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.addSource(new ResettableCustomSource()).print()

    env.execute()

  }
}
