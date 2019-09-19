package com.machinedoll.gate.serializer

import org.apache.flink.api.common.serialization.SerializationSchema

class AvroSerializationSchema[T]() extends SerializationSchema[T] {

  override def serialize(element: T): Array[Byte] = {
    ???
  }
}
