package com.blueegg.apitest

import java.util

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
/*
要求两个温度不能跳变

需求： 对于温度传感器温度值跳变，超过10度，报警
* */

object StateTest3 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream = env.socketTextStream("localhost", 7777)

    // 先转换成样例类类型
    val dataSteam = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    val alertStream = dataSteam
        .keyBy(_.id)   // 肯定是对同一个传感器
        .flatMapWithState[(String, Double, Double), Double]({ // 这个必须是键控状态才能使用
          case (data: SensorReading, None) => (List.empty, Some(data.temperature))
          case (data: SensorReading, lastTemp: Some[Double]) => {
            // 跟最新的温度值求差值作比较
            val diff = (data.temperature - lastTemp.get).abs
            if (diff > 10.0) {
              (List((data.id, lastTemp.get, data.temperature)), Some(data.temperature))
            } else {
              (List.empty, Some(data.temperature))
            }
          }
        })

    alertStream.print()
    env.execute("state test")
  }
}


