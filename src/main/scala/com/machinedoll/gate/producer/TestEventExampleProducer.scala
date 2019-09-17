package com.machinedoll.gate.producer

import com.machinedoll.gate.generator.SimpleSensorReadingGenerator
import com.machinedoll.gate.sink.SinkCollection
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestEventExampleProducer {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = ParameterTool.fromArgs(args)

    val conf = ConfigFactory.load()

    val sensorReading = env
      .addSource(new SimpleSensorReadingGenerator)

    sensorReading.print()

    sensorReading.addSink(SinkCollection.getKafkaAvroSink(conf, "kafka-avro-example-topic"))
    env.execute("Producer example")
  }
}
