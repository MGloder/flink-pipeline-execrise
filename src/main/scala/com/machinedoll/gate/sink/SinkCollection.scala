package com.machinedoll.gate.sink

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.lang
import java.util.{Optional, Properties}

import com.machinedoll.gate.partitioner.KafkaCustomPartitioner
import com.machinedoll.gate.schema.SensorReading
import com.sksamuel.avro4s.AvroSchema
import com.typesafe.config.Config
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificData
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.formats.avro.AvroRowSerializationSchema
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

object SinkCollection {
  def getKafkaAvroSink(config: Config, topic: String): FlinkKafkaProducer[SensorReading] = {
    val props = new Properties()
    props.setProperty("bootstrap.servers",
      config.getString("kafka.kafka-server"))
    props.setProperty("zookeeper.connect",
      config.getString("kafka.zookeeper-server"))
    props.setProperty("group.id",
      config.getString("kafka.group.id"))

    new FlinkKafkaProducer[SensorReading](topic, new KafkaSerializationSchema[SensorReading] {
      override def serialize(element: SensorReading, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
        try {
          val schema = AvroSchema[SensorReading]
          val writer = new GenericDatumWriter[GenericRecord](schema)
          val binaryEncoder = EncoderFactory.get().binaryEncoder(stream, null)
          val sensorReadingRecord = new GenericData.Record(schema)
          sensorReadingRecord.put("id", element.id)
          sensorReadingRecord.put("reading", element.reading)
          sensorReadingRecord.put("timestamp", element.timestamp)
          writer.write(sensorReadingRecord, binaryEncoder)
          binaryEncoder.flush()
          val serializedBytes: Array[Byte] = stream.toByteArray
          new ProducerRecord[Array[Byte], Array[Byte]](topic, serializedBytes)
        } catch {
          case e: Exception => {
            println(e.getMessage)
            new ProducerRecord[Array[Byte], Array[Byte]](topic, "".getBytes())
          }
        } finally {
          stream.close()
        }
      }
    }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
  }

//  def getKafkaAvroSpecificSink(config: Config, topic: String): FlinkKafkaProducer[SensorReading] = {
//    val props = new Properties()
//    props.setProperty("bootstrap.servers",
//      config.getString("kafka.kafka-server"))
//    props.setProperty("zookeeper.connect",
//      config.getString("kafka.zookeeper-server"))
//    props.setProperty("group.id",
//      config.getString("kafka.group.id"))
//
//    new FlinkKafkaProducer[SensorReading](
//      topic, new KafkaSerializationSchema[SensorReading] {
//        override def serialize(element: SensorReading, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
//          val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
//          try {
//            val schema = AvroSchema[SensorReading]
//            val writer = new GenericDatumWriter[SensorReading](schema)
//            val binaryEncoder = EncoderFactory.get().binaryEncoder(stream, null)
//            val sensorReadingWriter = new SpecificData().createDatumWriter(schema)
//            sensorReadingWriter.write(element, binaryEncoder)
////            sensorReadingWriter.write(element.asInstanceOf[SpecificData], binaryEncoder)
//            binaryEncoder.flush()
//            val serializedBytes: Array[Byte] = stream.toByteArray
//            new ProducerRecord[Array[Byte], Array[Byte]](topic, serializedBytes)
//          } catch {
//            case e: Exception => {
//              println(e.getMessage)
//              new ProducerRecord[Array[Byte], Array[Byte]](topic, "".getBytes())
//            }
//          } finally {
//            stream.close()
//          }
//        }
//      },
//      props,
//      FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
//  }


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

    new FlinkKafkaProducer[SensorReading](topic, new KafkaSerializationSchema[SensorReading] {
      override def serialize(element: SensorReading, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(stream)
        oos.writeObject(element)
        oos.close()
        val result = stream.toByteArray
        new ProducerRecord[Array[Byte], Array[Byte]](topic, result)
      }
    }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
  }
}
