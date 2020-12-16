package source

import org.apache.flink.streaming.api.scala._

object SocketTextStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 11111)
    stream.print()
    env.execute("socket job")
  }
}
