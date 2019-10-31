package com.machinedoll.gate.test

import com.machinedoll.gate.generator.SimpleSensorReadingGenerator
import com.machinedoll.gate.schema.{EventTest, SensorReading}
import com.typesafe.config.ConfigFactory
import javax.xml.transform.stream.StreamSource
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object ParquetAvroSinkExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(1000)

    val props = ParameterTool.fromArgs(args)

    val conf = ConfigFactory.load()

    val eventTestSource = env
      .addSource(new SimpleSensorReadingGenerator())

    val customParquetAvroFileSink = StreamingFileSink
      .forBulkFormat(
        new Path("/tmp/data"),
        ParquetAvroWriters.forReflectRecord(classOf[SensorReading])
      ).build()

    eventTestSource.addSink(customParquetAvroFileSink)

    env.execute("Parquet Avro Sink Example")
  }
}
