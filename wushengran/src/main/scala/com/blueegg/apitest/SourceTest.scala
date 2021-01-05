package com.blueegg.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

// 定义样例类，温度传感器
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1、集合中读取
//    val dataList = List(
//      SensorReading("sensor_1", 1547718199, 35.8),
//      SensorReading("sensor_6", 1547718201, 15.4),
//      SensorReading("sensor_7", 1547718202, 6.7),
//      SensorReading("sensor_10", 1547718205, 38.1)
//    )
//    val stream1 = env.fromCollection(dataList)
//    stream1.print()

    // 2、从文件读取
//    val inputPath = "/Users/cyd/Desktop/study/flink/flink-scala-examples/wushengran/src/main/resources/sensor.txt"
//    val stream2 = env.readTextFile(inputPath)
//    stream2.print()

    // 3、从kafka读取
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    val stream3 = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties))
    stream3.print()
    env.execute("source test")
  }

}
