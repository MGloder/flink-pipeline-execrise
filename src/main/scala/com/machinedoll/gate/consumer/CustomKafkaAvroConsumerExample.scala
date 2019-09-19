package com.machinedoll.gate.consumer

import com.machinedoll.gate.schema.SensorReading
import com.machinedoll.gate.source.SourceCollection
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object CustomKafkaAvroConsumerExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = ParameterTool.fromArgs(args)

    val conf = ConfigFactory.load()

    val sensorReading = env
      .addSource(SourceCollection.getKafkaAvroSensorReadingSource(conf, "example-topic2"))
      .map(
        g =>
          new SensorReading(
            g.get("id").toString,
            g.get("reading").toString.toFloat,
            g.get("timestamp").toString.toLong)
      )

    sensorReading.print()


    env.execute("Custom Kafka Consumer")
  }
}
