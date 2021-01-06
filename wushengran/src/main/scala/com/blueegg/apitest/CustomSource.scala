package com.blueegg.apitest

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

// 定义样例类，温度传感器
case class SensorReading(id: String, timestamp: Long, temperature: Double)


object CustomSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new MySensorSource())
    stream.print()
    env.execute()
  }
}

// 实现每次在上一次温度基础上，轻微扰动
class MySensorSource() extends SourceFunction[SensorReading]{
  // 定义一个标志位flag，用来表示数据源是否正常运行发出数据
  var running: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 定义一个随机数发生器
    val rand = new Random()

    // 随机生成一组(10个)传感器的初始温度: (id, temp)
    var curTemp = 1.to(10).map(i => ("sensor_" + i, rand.nextDouble() * 100))

    // 定义无限循环，不停地产生数据，除非被cancel
    while(running){
      // 在上次数据基础上微调，更新温度值
      curTemp = curTemp.map(
        data => (data._1, data._2 + rand.nextGaussian()) // 下一个高斯随机数，正太分布，在附近扰动
      )

      // 获取当前时间戳，加入到数据中，调用ctx发出数据
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        data => ctx.collect(SensorReading(data._1, curTime, data._2))
      )
      Thread.sleep(500)
    }
  }

  override def cancel(): Unit = running = false
}

// 自定义函数类
class MyFilter extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean =
    value.id.startsWith("sensor_1")
}

// 富函数，可以获取到运行时上下文，还有一些生命周期
class MyRichMapper extends RichMapFunction[SensorReading, String] {

  override def open(parameters: Configuration): Unit = {
    // 做一些初始化操作，比如数据库的连接
//    getRuntimeContext.getState()
  }

  override def map(in: SensorReading): String = in.id + "temperature"

  override def close(): Unit = {
    // 一般做收尾工作，比如关闭连接，或者清空状态
  }
}