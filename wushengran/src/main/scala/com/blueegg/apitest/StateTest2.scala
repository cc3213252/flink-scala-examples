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

object StateTest2 {
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
        .flatMap(new TempChangeAlert(10.0))

    alertStream.print()
    env.execute("state test")
  }
}


// 实现自定义RichFlatmapFunction
class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  // 定义状态保存上一次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valuestate", classOf[Double]))


  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上一次的温度值
    val lastTemp = lastTempState.value()
    // 跟最新的温度值求差值作比较
    val diff = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      out.collect((value.id, lastTemp, value.temperature))
    }
    // 更新状态
    lastTempState.update(value.temperature)
  }
}
