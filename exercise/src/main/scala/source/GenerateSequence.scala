package source

import org.apache.flink.streaming.api.scala._

object GenerateSequence {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1, 10)
    stream.print()
    env.execute("generate job")
  }
}
