package com.machinedoll.gate.consumer

import com.machinedoll.gate.source.SourceCollection
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import org.apache.flink.streaming.api.scala._

object CustomKafkaConsumerExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = ParameterTool.fromArgs(args)

    val conf = ConfigFactory.load()

    val sensorReading = env
      .addSource(SourceCollection.getKafkaSensorReadingSource(conf, "kafka-avro-example-topic"))

    sensorReading.print()


    env.execute("Custom Kafka Consumer")
  }
}
