package transformation

import org.apache.flink.streaming.api.scala._

object KeyBy {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.readTextFile("text00.txt").flatMap(_.split(" ")).map((_, 1L))
    val streamKeyBy = stream.keyBy(0)
    val streamReduce = streamKeyBy.reduce(
      (item1, item2) => (item1._1, item1._2 + item2._2)
    )
    streamReduce.print()
    env.execute("keyby job")
  }
}
