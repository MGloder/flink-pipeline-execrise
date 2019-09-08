package com.machinedoll.gate.test

import com.machinedoll.gate.generator.{SimpleSensorReadingGenerator, SimpleSequenceObjectGenerator}
import com.machinedoll.gate.source.SourceCollection
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object CustomKafkaPartitionerExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = ParameterTool.fromArgs(args)

    val conf = ConfigFactory.load()

    val eventTestSource = env.addSource(new SimpleSequenceObjectGenerator(1000, 10, 1))
    eventTestSource.print()

    env.execute("Custom Kafka Partitioner Example")
  }
}
