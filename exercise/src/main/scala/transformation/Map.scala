package transformation

import org.apache.flink.streaming.api.scala._

object Map {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1,10)
    val streamMap = stream.map(item => item*2)
    streamMap.print()
    env.execute("map job")
  }
}
