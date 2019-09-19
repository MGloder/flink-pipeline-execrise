package com.machinedoll.gate.source


import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util.Properties

import com.machinedoll.gate.schema.SensorReading
import com.sksamuel.avro4s.AvroSchema
import com.typesafe.config.Config
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.formats.avro.AvroDeserializationSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

object SourceCollection {

  def getKafkaJsonSourceTest(config: Config, topic: String) = {
    val props = new Properties()
    props.setProperty("bootstrap.servers",
      config.getString("kafka.kafka-server"))
    props.setProperty("zookeeper.connect",
      config.getString("kafka.zookeeper-server"))
    props.setProperty("group.id",
      config.getString("kafka.group.id"))
    // JSON
    new FlinkKafkaConsumer(topic,
      new JSONKeyValueDeserializationSchema(true),
      props)
  }

  def getKafkaSimpleStringSource(config: Config, topic: String): FlinkKafkaConsumer[String] = {
    val props = new Properties()
    props.setProperty("bootstrap.servers",
      config.getString("kafka.kafka-server"))
    //    props.setProperty("zookeeper.connect",
    //      config.getString("kafka.zookeeper-server"))
    //    props.setProperty("group.id",
    //      config.getString("kafka.group.id"))

    new FlinkKafkaConsumer(topic, new SimpleStringSchema(), props)
  }

  def getKafkaSensorReadingSource(config: Config, topic: String): FlinkKafkaConsumer[SensorReading] = {
    val props = new Properties()
    props.setProperty("bootstrap.servers",
      config.getString("kafka.kafka-server"))
    props.setProperty("zookeeper.connect",
      config.getString("kafka.zookeeper-server"))
    props.setProperty("group.id",
      config.getString("kafka.group.id"))

    val schema = AvroSchema[SensorReading]

    val consumer = new FlinkKafkaConsumer(topic,
      new KafkaDeserializationSchema[SensorReading] {
        override def isEndOfStream(nextElement: SensorReading): Boolean = false

        override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): SensorReading = {
          val ois = new ObjectInputStream(new ByteArrayInputStream(record.value()))
          val value = ois.readObject
          ois.close()
          value.asInstanceOf[SensorReading]
        }

        override def getProducedType: TypeInformation[SensorReading] = TypeInformation.of(classOf[SensorReading])
      },
      props
    )
    consumer.setStartFromEarliest()

    consumer
  }

  def getKafkaAvroSensorReadingSource(config: Config, topic: String): FlinkKafkaConsumer[GenericRecord] = {
    val props = new Properties()
    props.setProperty("bootstrap.servers",
      config.getString("kafka.kafka-server"))
    props.setProperty("zookeeper.connect",
      config.getString("kafka.zookeeper-server"))
    props.setProperty("group.id",
      config.getString("kafka.group.id"))

    val schema = AvroSchema[SensorReading]

//    val reader = new GenericDatumReader[GenericRecord](schema)

    new FlinkKafkaConsumer(
      topic,
      AvroDeserializationSchema.forGeneric(schema),
//      new KafkaDeserializationSchema[SensorReading] {
//        override def isEndOfStream(nextElement: SensorReading): Boolean = false
//
//        override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): SensorReading = {
//          val decoder = DecoderFactory.get.binaryDecoder(record.value(), null)
//          val generocRecord = reader.read(null, decoder)
//          new SensorReading(
//            id = generocRecord.get("id").toString,
//            reading = generocRecord.get("reading").toString.toFloat,
//            timestamp = generocRecord.get("timestamp").toString.toLong
//          )
//        }
//
//        override def getProducedType: TypeInformation[SensorReading] = TypeInformation.of(classOf[SensorReading])
//      },
      props
    )

  }
}
