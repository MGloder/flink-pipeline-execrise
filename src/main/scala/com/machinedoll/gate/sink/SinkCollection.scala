package com.machinedoll.gate.sink

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.lang
import java.util.{Optional, Properties}

import com.machinedoll.gate.partitioner.KafkaCustomPartitioner
import com.machinedoll.gate.schema.{EventTest, SensorReading}
import com.typesafe.config.Config
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

object SinkCollection {
  def getKafkaAvroSink(conf: Config, topic: String): FlinkKafkaProducer[SensorReading] = {
    ???
  }


  def getKafkaStringSinkTest(config: Config, topic: String): FlinkKafkaProducer[String] = {

    val props = new Properties()
    props.setProperty("bootstrap.servers",
      config.getString("kafka.kafka-server"))
    props.setProperty("zookeeper.connect",
      config.getString("kafka.zookeeper-server"))
    props.setProperty("group.id",
      config.getString("kafka.group.id"))

    new FlinkKafkaProducer[String](
      topic,
      new SimpleStringSchema(),
      props
    )

    // versions 0.10+ allow attaching the records' event timestamp when writing them to Kafka;
    // this method is not available for earlier Kafka versions
  }

  def getKafkaCustomPartitionSink(config: Config, topic: String): FlinkKafkaProducer[String] = {
    val props = new Properties()
    props.setProperty("bootstrap.servers",
      config.getString("kafka.kafka-server"))
    props.setProperty("zookeeper.connect",
      config.getString("kafka.zookeeper-server"))
    props.setProperty("group.id",
      config.getString("kafka.group.id"))

    val customKafkaPartitioner: Optional[FlinkKafkaPartitioner[String]] =
      Optional.of(new KafkaCustomPartitioner())
    new FlinkKafkaProducer(topic, new SimpleStringSchema(), props, customKafkaPartitioner)
  }

  def getKafkaCustomSerializerSink(config: Config, topic: String) = {
    val props = new Properties()
    props.setProperty("bootstrap.servers",
      config.getString("kafka.kafka-server"))
    props.setProperty("zookeeper.connect",
      config.getString("kafka.zookeeper-server"))
    props.setProperty("group.id",
      config.getString("kafka.group.id"))

    new FlinkKafkaProducer[EventTest](topic, new KafkaSerializationSchema[EventTest] {
      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(stream)

      override def serialize(element: EventTest, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        oos.writeObject(element)
        oos.close()
        val result = stream.toByteArray
        new ProducerRecord[Array[Byte], Array[Byte]](topic, result)
      }
    }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
  }
}
