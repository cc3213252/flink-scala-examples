package com.blueegg.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object KafkaPipelineTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 2.2 从kafka读取数据
    tableEnv.connect(new Kafka()
      .version("universal")
      .topic("sensor")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

    // 3. 查询转换
    // 3.1 简单转换
    val sensorTable = tableEnv.from("kafkaInputTable")
    val resultTable = sensorTable
      .select($"id", $"temperature")
      .filter($"id" === "sensor_1")

    // 3.2 聚合转换
    val aggTable = sensorTable
      .groupBy('id) // 基于id分组
      .select('id, 'id.count() as 'count)

    // 4 输出到kafka
    tableEnv.connect(new Kafka()
      .version("universal")
      .topic("sinktest")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaOutputTable")

    resultTable.executeInsert("kafkaOutputTable")
  }
}
