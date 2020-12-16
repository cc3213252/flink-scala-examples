package source

import org.apache.flink.streaming.api.scala._

object FromCollection {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val list = List(1,2,3,4) // 或Iterator
    val stream = env.fromCollection(list)
    stream.print()
    env.execute("collection job")
  }
}
