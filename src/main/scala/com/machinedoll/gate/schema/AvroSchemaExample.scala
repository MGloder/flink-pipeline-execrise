package com.machinedoll.gate.schema

import scala.io.Source

case class AvroSchemaExample() {
  val record: String = Source.fromFile("/Users/xyan/Projects/flink-pipeline-execrise/src/main/resources/user.avsc")
    .getLines
    .mkString

  val exampleEvent: String = Source
    .fromFile("/Users/xyan/Projects/flink-pipeline-execrise/src/main/resources/example_event.avsc")
    .getLines()
    .mkString

  def printSchema = println(record)
}
