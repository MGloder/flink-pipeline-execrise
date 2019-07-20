package com.machinedoll.gate.source


import java.util.Properties

import com.typesafe.config.Config
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

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
    // POJO

//    new KafkaConsumer[EventTest, ](topic, new DeserializationSchema[EventTest] {
//      override def deserialize(bytes: Array[Byte]): EventTest = ???
//
//      override def isEndOfStream(t: EventTest): Boolean = ???
//
//      override def getProducedType: TypeInformation[EventTest] = ???
//    }, props)
  }
}
