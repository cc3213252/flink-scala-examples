package com.blueegg.apitest

import java.util
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(1000L)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000L) // 1分钟
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)   // 最小间隔时间，这个和上面这个配一个即可
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(2) // 要容忍多少次checkpoint失败

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 60000L)) // 固定时间间隔重启
    env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))

    val inputStream = env.socketTextStream("localhost", 7777)

    // 先转换成样例类类型
    val dataSteam = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    env.execute("state test")
  }
}


// keyed state测试： 必须定义在RichFunction中，因为需要运行时上下文
class MyRichMapper2 extends RichMapFunction[SensorReading, String] {
  var valueState: ValueState[Double] = _
  lazy val listState: ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("liststate", classOf[Int]))
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String,Double]("mapstate", classOf[String], classOf[Double]))
  lazy val reduceState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("reductstate", new MyReducer, classOf[SensorReading]))
  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valuestate", classOf[Double]))
  }

  override def map(value: SensorReading): String = {
    // 状态的读写
    val myV = valueState.value()
    valueState.update(value.temperature)
    listState.add(1)

    val list = new util.ArrayList[Int]()
    list.add(2)
    list.add(3)
    listState.addAll(list)

    mapState.contains("sensor_1")
    mapState.get("sensor_1")
    mapState.put("sensor_1", 1.3)

    reduceState.get()
    reduceState.add(value) // 做聚合
    value.id
  }
}