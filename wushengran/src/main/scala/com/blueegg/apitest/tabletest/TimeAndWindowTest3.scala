package com.blueegg.apitest.tabletest

import com.blueegg.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/*
结果完全一样的方案
每个窗口到点输出一次结果
 */
object TimeAndWindowTest3 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val filePath = "/Users/cyd/Desktop/study/flink/flink-scala-examples/wushengran/src/main/resources/sensor.txt"
    val inputStream = env.readTextFile(filePath)
    // 先转换成样例类类型
    val dataSteam = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000L
        }
      })

    val sensorTable = tableEnv.fromDataStream(dataSteam, 'id, 'temperature, 'timestamp.rowtime as 'ts)
//    sensorTable.printSchema()
//    sensorTable.toAppendStream[Row].print()

    // 1 Group Window 窗口分组测试第86视频
    // 1. table api  'tw.end 当前窗口结束时间
    val resultTable = sensorTable
        .window(Tumble over 10.seconds on 'ts as 'tw)  // 每10秒统计一次，滚动时间窗口
        .groupBy('id, 'tw)
        .select('id, 'id.count, 'temperature.avg, 'tw.end)

    // 2 sql
    tableEnv.createTemporaryView("sensor", sensorTable)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select
        |  id,
        |  count(id),
        |  avg(temperature),
        |  tumble_end(ts, interval '10' second)
        |from sensor
        |group by
        |  id,
        |  tumble(ts, interval '10' second)
        |""".stripMargin)

    // 转换成流，打印输出
    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toRetractStream[Row].print("sql")
    env.execute("table api")
  }
}
