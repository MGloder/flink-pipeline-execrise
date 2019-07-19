package com.machinedoll.gate.source


import java.util.Properties

import com.machinedoll.gate.schema.EventTest
import com.typesafe.config.Config
import org.apache.flink.api.common.serialization.SimpleStringSchema
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

//    new KafkaConsumer[EventTest](topic, new SimpleStringSchema(), props)

  }
}
