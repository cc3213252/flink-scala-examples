package com.blueegg.apitest.sinktest

import com.blueegg.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

object FileSink {
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
    dataSteam.print()
//    dataSteam.writeAsCsv("/Users/cyd/Desktop/study/flink/flink-scala-examples/wushengran/src/main/resources/out.txt")
    dataSteam.addSink(
      StreamingFileSink.forRowFormat(
        new Path("/Users/cyd/Desktop/study/flink/flink-scala-examples/wushengran/src/main/resources/out1.txt"),
        new SimpleStringEncoder[SensorReading]()
      ).build()
    )
    env.execute()
  }
}


