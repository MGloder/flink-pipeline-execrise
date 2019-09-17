package com.machinedoll.gate.producer

import java.util.Properties

import com.machinedoll.gate.schema.EventTest
import org.apache.kafka.clients.producer.{Producer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer


object TestEventExampleProducer {
  private def createEventTestProducer: Producer[String, EventTest] = {
    val props: Properties = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  }

  def main(args: Array[String]) = {

  }
}
