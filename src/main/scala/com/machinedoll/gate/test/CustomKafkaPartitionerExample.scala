package com.machinedoll.gate.test

import com.machinedoll.gate.generator.{SimpleSensorReadingGenerator, SimpleSequenceObjectGenerator, SimpleSequenceStringGenerator}
import com.machinedoll.gate.schema.EventTest
import com.machinedoll.gate.sink.SinkCollection
import com.machinedoll.gate.source.SourceCollection
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object CustomKafkaPartitionerExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = ParameterTool.fromArgs(args)

    val conf = ConfigFactory.load()

    val eventTestSource = env
      .addSource(new SimpleSequenceStringGenerator(1000, 100))

    eventTestSource.print()
    val result = eventTestSource.addSink(SinkCollection.getKafkaStringSinkTest(conf, "event-test"))

    env.execute("Custom Kafka Partitioner Example")
  }
}
