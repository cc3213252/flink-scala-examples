package source

import org.apache.flink.streaming.api.scala._

object ReadTextFile {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.readTextFile("README.md")
    stream.print()
    env.execute("FirstJob")
  }
}
