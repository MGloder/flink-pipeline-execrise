package com.machinedoll.gate.partitioner

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner

class KafkaCustomPartitioner extends FlinkKafkaPartitioner[String] {
  override def partition(record: String, key: Array[Byte], value: Array[Byte], targetTopic: String, partitions: Array[Int]): Int = {
    val modifier = 2
    println("Found paritions: " + partitions.size)
    val parition = record.toLong % 2 match {
      case 0 => partitions(0)
      case 1 => partitions(1)
      case _ => partitions(2)
    }
    println("Will be save to parition: " + parition)
    parition
  }
}
