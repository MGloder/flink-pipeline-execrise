package com.machinedoll.gate.algorithm

import com.machinedoll.gate.generator.SimpleSequenceObjectGenerator
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import org.apache.flink.api.scala._

object CalculateFrequencyMoments {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env
      .addSource(new SimpleSequenceObjectGenerator(1000, 1, 1))
      .print()

    env.execute("Sample Sequence object gen")
  }
}
