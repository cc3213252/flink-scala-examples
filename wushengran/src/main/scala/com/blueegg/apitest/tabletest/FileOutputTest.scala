package com.blueegg.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{Csv, FileSystem, OldCsv, Schema}

object FileOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)
    val filePath = "/Users/cyd/Desktop/study/flink/flink-scala-examples/wushengran/src/main/resources/sensor.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
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

//    resultTable.toAppendStream[(String, Double)].print("result")
//    aggTable.toRetractStream[(String, Long)].print("agg") // false表示这条数据失效

    // 4 输出到文件
    // 注册输出表
    val outputPath = "/Users/cyd/Desktop/study/flink/flink-scala-examples/wushengran/src/main/resources/output.txt"

    tableEnv.connect(new FileSystem().path(outputPath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("outputTable")

    resultTable.executeInsert("outputTable") // 版本升级，方法不存在，改用executeInsert
  }
}
