/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.machinedoll.gate.test.join

import com.google.gson.Gson
import com.machinedoll.gate.generator.SimpleSequenceObjectGenerator
import com.machinedoll.gate.schema.EventTest
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

object TwoStream {
  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.parseResources("connection.conf")
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // set parallelism
    env.setParallelism(1)

    println("Current parallelism: [ " + env.getParallelism + " ]")

    val randomEvent: DataStream[EventTest] = env
      .addSource(new SimpleSequenceObjectGenerator(1000, 10, numDes = 10))
//      .assignTimestampsAndWatermarks(new PeriodicWaterarkExample)

    val keyByEvent = randomEvent
//      .assignTimestampsAndWatermarks(new PeriodicWaterarkExample)
      .keyBy(_.id)

//    keyByEvent.print()

    val processedKeyByEvent = keyByEvent.process(new KeyedProcessFunctionExample)
    processedKeyByEvent.print()

    env.execute()
  }
}

class CustomJsonConverter[EventTest]() extends FlatMapFunction[EventTest, String] {
  override def flatMap(t: EventTest, collector: Collector[String]): Unit = {
    val gson = new Gson
    val evnetTest: String = gson.toJson(t)
    collector.collect(evnetTest)
  }
}

class PeriodicWaterarkExample extends AssignerWithPeriodicWatermarks[EventTest] {
  val bound: Long = 1000 * 60 // 1 minus

  var maxWs: Long = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxWs - bound)
  }

  override def extractTimestamp(t: EventTest, l: Long): Long = {
    maxWs = maxWs.max(t.timestamp)
    t.timestamp
  }
}

class PunctuatedWatermarkExample extends AssignerWithPunctuatedWatermarks[EventTest] {
  override def checkAndGetNextWatermark(t: EventTest, l: Long): Watermark = ???

  override def extractTimestamp(t: EventTest, l: Long): Long = ???
}

case class CountWithTimestamp(id: Int, count: Int, lastModified: Long)
case class CountResult(id: Int, num: Int)

class KeyedProcessFunctionExample extends  KeyedProcessFunction[Int, EventTest, CountResult] {

  lazy val state: ValueState[CountWithTimestamp] = getRuntimeContext
    .getState(new ValueStateDescriptor[CountWithTimestamp]("myState", classOf[CountWithTimestamp]))

  override def processElement(value: EventTest,
                              ctx: KeyedProcessFunction[Int, EventTest, CountResult]#Context,
                              out: Collector[CountResult]): Unit = {
    println(value)
    val current: CountWithTimestamp = state.value match {
      case null => CountWithTimestamp(value.id, 1, ctx.timestamp)
      case CountWithTimestamp(id, count, lastModified) => CountWithTimestamp(id, count + 1, ctx.timestamp())
    }


    state.update(current)

    ctx.timerService.registerEventTimeTimer(current.lastModified + 60000)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Int, EventTest, CountResult]#OnTimerContext,
                       out: Collector[CountResult]): Unit = {

    state.value match {
      case CountWithTimestamp(id, count, lastModified) if (timestamp == lastModified + 60000) =>
        out.collect(CountResult(id, count))
      case _ =>
    }
  }
}


