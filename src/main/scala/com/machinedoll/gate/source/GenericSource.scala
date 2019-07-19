package com.machinedoll.gate.source


import java.util.Properties

import com.machinedoll.gate.schema.EventTest
import com.typesafe.config.Config
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.kafka.clients.consumer.KafkaConsumer

object GenericSource {

  def getKafkaSource(config: Config, topic: String) = {
    val props = new Properties()
    props.setProperty("bootstrap.servers",
      config.getString("kafka.kafka-server"))
    props.setProperty("zookeeper.connect",
      config.getString("kafka.zookeeper-server"))
    props.setProperty("group.id",
      config.getString("kafka.group.id"))
    props.setProperty("enable.auto")
    // JSON
    new KafkaConsumer[String, EventTest](props)

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
