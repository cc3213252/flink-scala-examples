package com.blueegg.apitest.tabletest

import com.blueegg.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala._


object ExampleSql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputPath = "/Users/cyd/Desktop/study/flink/flink-scala-examples/wushengran/src/main/resources/sensor.txt"
    val inputStream = env.readTextFile(inputPath)
    // 先转换成样例类类型
    val dataSteam = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    // 首先创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    // 基于流创建一张表
    val dataTable: Table = tableEnv.fromDataStream(dataSteam)


    // 直接用sql实现
    tableEnv.createTemporaryView("dataTable", dataTable)
    val sql: String = "select id, temperature from dataTable where id = 'sensor_1'"
    val resultSqlTable = tableEnv.sqlQuery(sql)
    resultSqlTable.toAppendStream[(String, Double)].print("result")

    env.execute("table api example")
  }
}
