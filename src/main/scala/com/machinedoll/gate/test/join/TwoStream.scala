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
import com.machinedoll.gate.sink.SinkCollection
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.functions.{FlatMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object TwoStream {
  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.parseResources("connection.conf")
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // set parallelism
    env.setParallelism(1)

    println("Current parallelism: [ " + env.getParallelism + " ]")

//    val exampleSource = SourceCollection.getKafkaJsonSourceTest(config, "test")
//    env.socketTextStream("127.0.0.1", 9999).print()

    val randomEvent: DataStream[EventTest] = env
      .addSource(new SimpleSequenceObjectGenerator(1000, 10, numDes = 10))
//      .assignTimestampsAndWatermarks(new PeriodicWaterarkExample)

    randomEvent
      .assignTimestampsAndWatermarks(new PeriodicWaterarkExample)
      .keyBy(_.id)
      .process(new KeyedProcessFunctionExample)
      .print()

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

case class CustomResult(id: Int, num: Int, lastModified: Long) extends Serializable
case class CustomResultMap(id: Int, num: Int) extends Serializable

class KeyedProcessFunctionExample extends KeyedProcessFunction[Int, EventTest, CustomResult] {

  lazy val state: ValueState[CustomResult] = getRuntimeContext
    .getState(new ValueStateDescriptor[CustomResult]("myState", classOf[CustomResult]))

  override def processElement(i: EventTest,
                              context: KeyedProcessFunction[Int, EventTest, CustomResult]#Context,
                              collector: Collector[CustomResult]): Unit = {

    val count: CustomResult = state.value match {
      case null => CustomResult(i.id, 1, context.timestamp())
      case CustomResult(key, count, modified) => CustomResult(i.id, count + 1, context.timestamp())
    }

    state.update(count)

    context.timerService().registerEventTimeTimer(count.lastModified + 6000)
  }




  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Int, EventTest, CustomResult]#OnTimerContext,
                       out: Collector[CustomResult]): Unit = {
    state.value match {
      case CustomResult(key, count, lastModified) if (timestamp == lastModified + 6000)  => out.collect(CustomResult(key, count, lastModified))
      case _ =>
    }
  }
}


