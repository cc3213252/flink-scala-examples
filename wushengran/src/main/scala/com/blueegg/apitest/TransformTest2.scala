package com.blueegg.apitest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

object TransformTest2 {
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

    // 分组聚合，输出每个传感器当前最小值
    val aggStream = dataSteam
      .keyBy("id") // 根据id进行分组
      .min("temperature")

    // 需要输出当前最小的温度值，以及最近的时间戳，要用reduce
    val resultStream = dataSteam
        .keyBy("id")
//        .reduce((curState, newData) =>
//          SensorReading(curState.id, newData.timestamp, curState.temperature.min(newData.temperature)))
        .reduce(new MyReduceFunction())
    resultStream.print()
    env.execute("transform test")
  }

}

class MyReduceFunction extends ReduceFunction[SensorReading] {
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading =
    SensorReading(t.id, t1.timestamp, t.temperature.min(t1.temperature))
}
