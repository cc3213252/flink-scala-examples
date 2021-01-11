package com.blueegg.apitest

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream = env.socketTextStream("localhost", 7777)

    // 先转换成样例类类型
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    val highTempStream = dataStream
        .process(new SplitTempProcessor(30.0))

    highTempStream.print("high")
    highTempStream.getSideOutput(new OutputTag[(String, Long, Double)]("low")).print("low")
    env.execute("side output test")
  }
}


// 实现自定义ProcessFunction，进行分流
class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    // 如果当前温度值大于30，那么输出到主流
    if (value.temperature > threshold) {
      out.collect(value)
    } else {
      // 如果不超过30度，那么输出到侧输出流
      ctx.output(new OutputTag[(String, Long, Double)]("low"), (value.id, value.timestamp, value.temperature))
    }
  }
}
