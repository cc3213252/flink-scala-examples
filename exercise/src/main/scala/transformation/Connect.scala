package transformation

import org.apache.flink.streaming.api.scala._

object Connect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream01 = env.generateSequence(1, 10)
    val stream02 = env.readTextFile("text00.txt").flatMap(item => item.split(" "))
    val streamConnect = stream01.connect(stream02)
    val streamComap = streamConnect.map(item => item * 2, item => (item, 1L))
    streamComap.print()
    env.execute("connect job")
  }
}
