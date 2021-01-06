package com.blueegg.apitest.sinktest

import java.util.Properties

import com.blueegg.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object KafkaSinkTest2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    val stream = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties))

    // 先转换成样例类类型
    val dataSteam = stream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
      })
    dataSteam.addSink(new FlinkKafkaProducer[String]("localhost:9092", "sinktest", new SimpleStringSchema()))
    dataSteam.print()
    env.execute("kafka sink")
  }
}
