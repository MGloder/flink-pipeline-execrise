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
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
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

    // set parallelism
    env.setParallelism(1)

    println("Current parallelism: [ " + env.getParallelism + " ]")

//    val exampleSource = SourceCollection.getKafkaJsonSourceTest(config, "test")
//    env.socketTextStream("127.0.0.1", 9999).print()

    val randomEvent = env.addSource(new SimpleSequenceObjectGenerator(1000, 1, numDes = 10))

    randomEvent
      .flatMap(new CustomJsonConverter[EventTest]())
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



