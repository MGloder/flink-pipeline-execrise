package com.machinedoll.gate.sink

import java.util.Properties

import com.typesafe.config.Config
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object SinkCollection {
  def getKafkaJsonSinkTest(config: Config, topic: String): FlinkKafkaProducer[String] = {

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

  def getKafkaCustomPartitionSink(config: Config, topic: String) = {
    val props = new Properties()
    props.setProperty("bootstrap.servers",
      config.getString("kafka.kafka-server"))
    props.setProperty("zookeeper.connect",
      config.getString("kafka.zookeeper-server"))
    props.setProperty("group.id",
      config.getString("kafka.group.id"))

    new FlinkKafkaProducer(topic, new SimpleStringSchema(), props)
  }


}
