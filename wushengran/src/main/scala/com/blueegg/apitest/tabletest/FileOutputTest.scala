package com.blueegg.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

object FileOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)
    val filePath = "/Users/cyd/Desktop/study/flink/flink-scala-examples/wushengran/src/main/resources/sensor.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new OldCsv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    // 3.1 简单转换
    val sensorTable = tableEnv.from("inputTable")
    val resultTable = sensorTable
      .select($"id", $"temperature")
      .filter($"id" === "sensor_1")

    // 3.2 聚合转换
    val aggTable = sensorTable
      .groupBy('id) // 基于id分组
      .select('id, 'id.count() as 'count)

    resultTable.toAppendStream[(String, Double)].print("result")
    aggTable.toRetractStream[(String, Long)].print("agg") // false表示这条数据失效
    env.execute()
  }
}
