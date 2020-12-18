package window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TimeWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 11111)
    val streamKeyBy = stream.map((_, 1L)).keyBy(0)
    val streamWindow = streamKeyBy.timeWindow(Time.seconds(5)).reduce(
      (item1, item2) => (item1._1, item1._2 + item2._2)
    )
    streamWindow.print()
    env.execute("time job")
  }
}
