package com.machinedoll.gate.generator

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class BoundedOutOfOrdernessGenrator extends AssignerWithPeriodicWatermarks[ExampleEvent]{
  val maxOutOfOrderness = 3500L

  var currentMaxTimestamp: Long = 0L

  override def getCurrentWatermark: Watermark = {
    new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }

  override def extractTimestamp(element: ExampleEvent, previousElementTimestamp: Long): Long = {
    val timestamp = element.timestamp
    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
    timestamp
  }


}

case class ExampleEvent(id:Int, timestamp: Long)
