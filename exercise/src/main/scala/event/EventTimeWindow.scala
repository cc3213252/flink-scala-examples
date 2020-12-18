package event

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object EventTimeWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.socketTextStream("localhost", 11111).assignTimestampsAndWatermarks(
      // 延迟时间设置为0
      new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(0)) {
        override def extractTimestamp(element: String): Long = {
          // 输入数据格式：eventTime（毫秒） word
          val eventTime = element.split(" ")(0).toLong
          println(eventTime)
          eventTime
        }
      }
    ).map(item => (item.split(" ")(1), 1L)).keyBy(0)
    val streamWindow = stream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
    val streamReduce = streamWindow.reduce(
      (item1, item2) => (item1._1, item1._2 + item2._2)
    )
    streamReduce.print()
    env.execute("event job")
  }
}
