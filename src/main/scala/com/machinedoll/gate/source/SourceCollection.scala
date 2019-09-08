package com.machinedoll.gate.source


import java.util.Properties

import com.typesafe.config.Config
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
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
  }

  def getKafkaSimpleStringSource(config: Config, topic: String): FlinkKafkaConsumer[String] = {
    val props = new Properties()
    props.setProperty("bootstrap.servers",
      config.getString("kafka.kafka-server"))
    props.setProperty("zookeeper.connect",
      config.getString("kafka.zookeeper-server"))
    props.setProperty("group.id",
      config.getString("kafka.group.id"))

    new FlinkKafkaConsumer(topic, new SimpleStringSchema(), props)
  }
}
