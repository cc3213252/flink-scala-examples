package com.blueegg.apitest

import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputPath = "/Users/cyd/Desktop/study/flink/flink-scala-examples/wushengran/src/main/resources/sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    // 先转换成样例类类型
    val dataSteam = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    // 分组聚合，输出每个传感器当前最小值
    val aggStream = dataSteam
      .keyBy("id") // 根据id进行分组
      .min("temperature")

    aggStream.print()
    env.execute("transform test")
  }

}
