package transformation

import org.apache.flink.streaming.api.scala._

object Split {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.readTextFile("text00.txt")
    val streamFlatMap = stream.flatMap(item => item.split(" "))

    streamFlatMap.print()
//    val streamSplit = streamFlatMap.split(
//      word => {
//        (word.equals("very")) match {
//          case true => List("very")
//          case false => List("other")
//        }
//      }
//    )
//    val streamSelect01 = streamSplit.select("very")
//    val streamSelect02 = streamSplit.select("other")
    env.execute("split job")
  }
}
