package com.blueegg.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object TableQuery {
  def main(args: Array[String]): Unit = {
    // 1、创建环境
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
    // 3.1 使用table api
    val sensorTable = tableEnv.from("kafkaInputTable")
    val resultTable = sensorTable
        .select($"id", $"temperature")
        .filter($"id" === "sensor_1")

    // 3.2 SQL
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id, temperature
        |from kafkaInputTable
        |where id = 'sensor_1'
        |""".stripMargin)

    resultTable.toAppendStream[(String, Double)].print("result")
    resultSqlTable.toAppendStream[(String, Double)].print("sql")
    env.execute("test")
  }
}
