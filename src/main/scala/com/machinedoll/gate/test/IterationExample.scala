package com.machinedoll.gate.test

import com.typesafe.config.ConfigFactory
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object IterationExample {
  def main(args: Array[String]) : Unit = {
    val config = ConfigFactory.parseResources("connection.conf")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val someIntegers: DataStream[Long] = env.generateSequence(0, 100000000L)

    val iteratedStream = someIntegers.iterate(
      iteration => {
        val minusOne = iteration.map( v => v - 1)
        val stillGreaterThanZero = minusOne.filter (_ > 0)
        val lessThanZero = minusOne.filter(_ <= 0)
        (lessThanZero, stillGreaterThanZero)
      }
    ).timeWindowAll(Time.seconds(1)).sum(0)

    iteratedStream.print

    env.execute("Iterative")
  }
}
