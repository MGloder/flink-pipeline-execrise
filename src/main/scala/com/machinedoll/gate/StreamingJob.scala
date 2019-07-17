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

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
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
//  def main(args: Array[String]) {
//    // set up the streaming execution environment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    val hostName = args(0)
//    val port = args(1).toInt
//
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    val input = env.socketTextStream(hostName, port)
//
//    val inputMap = input.map(f => {
//      val arr = f.split("\\W+")
//      val code = arr(0)
//      val time = arr(1).toLong
//      (code, time)
//    })
//
//    val watermark = inputMap.assignTimestampsAndWatermarks(
//      new AssignerWithPeriodicWatermarks[(String, Long)] {
//        var currentMaxTimestamp = 0L
//        val maxOutOfOrderness = 10000L
//
//        var a: Watermark = null
//
//        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
//
//        override def getCurrentWatermark: Watermark = {
//          a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
//          a
//        }
//
//        override def extractTimestamp(t: (String, Long), l: Long): Long = {
//          val timestamp = t._2
//          currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
//          println("timestamp:" + t._1 +","+ t._2 + "|" +format.format(t._2) +","+  currentMaxTimestamp + "|"+ format.format(currentMaxTimestamp) + ","+ a.toString)
//          timestamp
//        }
//      }
//    )
//
//    val window = watermark
//        .keyBy(_._1)
//        .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//        .apply(new WindowFunctionTest)
//
//    window.print()
//
//    /*
//     * Here, you can start creating your execution plan for Flink.
//     *
//     * Start with getting some data from the environment, like
//     *  env.readTextFile(textPath);
//     *
//     * then, transform the resulting DataStream[String] using operations
//     * like
//     *   .filter()
//     *   .flatMap()
//     *   .join()
//     *   .group()
//     *
//     * and many more.
//     * Have a look at the programming guide:
//     *
//     * http://flink.apache.org/docs/latest/apis/streaming/index.html
//     *
//     */
//
//    // execute program
//    env.execute("Flink Streaming Scala API Skeleton")
//  }
def main(args: Array[String]): Unit = {
  if (args.length != 2) {
    System.err.println("USAGE:\nSocketWatermarkTest <hostname> <port>")
    return
  }

  val hostName = args(0)
  val port = args(1).toInt

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val input = env.socketTextStream(hostName,port)

  val inputMap = input.map(f=> {
    val arr = f.split("\\W+")
    val code = arr(0)
    val time = arr(1).toLong
    (code,time)
  })

  val watermark = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String,Long)] {

    var currentMaxTimestamp = 0L
    val maxOutOfOrderness = 10000L//最大允许的乱序时间是10s

    var a : Watermark = null

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    override def getCurrentWatermark: Watermark = {
      a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      a
    }

    override def extractTimestamp(t: (String,Long), l: Long): Long = {
      val timestamp = t._2
      println("max timestamp: " + t._1 + " timestamp: " + timestamp + " Current max timestamp: " + currentMaxTimestamp )
      currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
      println("timestamp:" + t._1 +","+ t._2 + "|" +format.format(t._2) +","+  currentMaxTimestamp + "|"+ format.format(currentMaxTimestamp) + ","+ a.toString)
      timestamp
    }
  })

  val window = watermark
    .keyBy(_._1)
    .window(TumblingEventTimeWindows.of(Time.seconds(3)))
    .apply(new WindowFunctionTest)

  window.print()

  env.execute()
}

  class WindowFunctionTest extends WindowFunction[(String,Long),(String, Int,String,String,String,String),String,TimeWindow]{

    override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int,String,String,String,String)]): Unit = {
      val list = input.toList.sortBy(_._2)
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      out.collect(key,input.size,format.format(list.head._2),format.format(list.last._2),format.format(window.getStart),format.format(window.getEnd))
    }

  }
}
