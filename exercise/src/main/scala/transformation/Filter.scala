package transformation

import org.apache.flink.streaming.api.scala._

object Filter {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1, 10)
    val streamFilter = stream.filter(item => item != 1)
    streamFilter.print()
    env.execute("filter job")
  }
}
