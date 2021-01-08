package com.blueegg.apitest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.StateTtlConfig.TtlTimeCharacteristic
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import sun.util.resources.cldr.chr.TimeZoneNames_chr

object WindowTest2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(500)
    val inputStream = env.socketTextStream("localhost", 7777)

    // 先转换成样例类类型
    val dataSteam = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })
    val latetag = new OutputTag[(String, Double, Long)]("late")
    // 每15秒统计一次，窗口内各传感器所有温度的最小值，以及最新的时间戳
    val resultStream = dataSteam
      .map(data => (data.id, data.temperature, data.timestamp))
      .keyBy(_._1) // 按照二元组的第一个元素（id）分组
      .timeWindow(Time.seconds(15))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(latetag)
      .reduce((curRes, newData) => (curRes._1, curRes._2.min(newData._2), newData._3))

    resultStream.getSideOutput(latetag).print("late")
    resultStream.print("result")
    env.execute()
  }
}

