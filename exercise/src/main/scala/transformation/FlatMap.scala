package transformation

import org.apache.flink.streaming.api.scala._

object FlatMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.readTextFile("text00.txt")
    val streamFlat = stream.flatMap(item => item.split(" "))
    streamFlat.print()
    env.execute("flatmap job")
  }
}
