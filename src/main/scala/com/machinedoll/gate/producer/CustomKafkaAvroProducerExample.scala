package com.machinedoll.gate.producer

import com.machinedoll.gate.generator.SimpleSensorReadingGenerator
import com.machinedoll.gate.sink.SinkCollection
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CustomKafkaAvroProducerExample {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load()

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sensorReading = env
      .addSource(new SimpleSensorReadingGenerator)

    sensorReading.print()

    sensorReading.addSink(SinkCollection.getKafkaAvroSink(conf, "example-topic2"))

    env.execute("Avro Example")
  }
}
