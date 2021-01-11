package com.blueegg.apitest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionTest {
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
      .keyBy(_.id)
      .process(new MyKeyedProcessFunction)


  }
}

class MyKeyedProcessFunction extends KeyedProcessFunction[String, SensorReading, String] {
  var myState: ValueState[Int] = _


  override def open(parameters: Configuration): Unit = {
    myState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("mystate", classOf[Int]))
  }

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    ctx.getCurrentKey
    ctx.timestamp()
    ctx.timerService().currentWatermark()
    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 60000L) //注册一个一分钟后的定时器
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {

  }
}
