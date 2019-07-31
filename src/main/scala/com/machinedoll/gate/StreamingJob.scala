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

package com.machinedoll.gate

import com.google.gson.Gson
import com.machinedoll.gate.generator.SimpleSequenceObjectGenerator
import com.machinedoll.gate.schema.EventTest
import com.machinedoll.gate.sink.SinkCollection
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
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
object StreamingJob {
  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.parseResources("connection.conf")
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    val exampleSource = SourceCollection.getKafkaJsonSourceTest(config, "test")
//    env.socketTextStream("127.0.0.1", 9999).print()

//    val randomEvent = env.addSource(new SimpleSequenceObjectGenerator(100, 1000, 1000))
//
//    randomEvent
//      .flatMap(new CustomJsonConverter[EventTest]())
//      .addSink(SinkCollection.getKafkaJsonSinkTest(config, "event_test_topic"))
//  val input = env.socketTextStream(hostName,port)
//
//  val inputMap = input.map(f=> {
//    val arr = f.split("\\W+")
//    val code = arr(0)
//    val time = arr(1).toLong
//    (code,time)
//  })
//
//  val watermark = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String,Long)] {
//
//    var currentMaxTimestamp = 0L
//    val maxOutOfOrderness = 10000L//最大允许的乱序时间是10s
//
//    var a : Watermark = null
//
//    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
//
//    override def getCurrentWatermark: Watermark = {
//      a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
//      a
//    }
//
//    override def extractTimestamp(t: (String,Long), l: Long): Long = {
//      val timestamp = t._2
//      println("max timestamp: " + t._1 + " timestamp: " + timestamp + " Current max timestamp: " + currentMaxTimestamp )
//      currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
//      println("timestamp:" + t._1 +","+ t._2 + "|" +format.format(t._2) +","+  currentMaxTimestamp + "|"+ format.format(currentMaxTimestamp) + ","+ a.toString)
//      timestamp
//    }
//  })
//
//  val window = watermark
//    .keyBy(_._1)
//
//    .allowedLateness(Time.seconds(3))
//    .apply(new WindowFunctionTest)

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



