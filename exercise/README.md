## 本节内容为b站尚硅谷视频练习

https://www.bilibili.com/video/BV1nt41137FD?p=6

## 新建

scala上右键Scala class，选object  

## 快捷命令

main会联想快捷生成  

## 注意点

import org.apache.flink.streaming.api.scala._
用下划线全部包含进来，如果不这样做，之后使用算子的时候，可能由于隐式转换的原因出错  

## 测试socket

打开： nc -l 11111
输入一些字符  

## 问题

Split中没有split这个方法  
timeWindow报错  

## KeyBy

同一个key对应的数据会在同一个线程中，说明keyby会把同一个key分到同一个分区中  
结果说明flink会把每一个中间步骤都输出，如果想在flink做聚合操作，必须引入窗口概念  

## countWindow

是指同一个key的频次  
countWindow(5, 2) 首先要满足滑动参数，触发时如果窗口参数也符合就按窗口参数统计，不符合按滑动参数统计
  触发后参数重置  

timeWindow(Time.seconds(10), Time.seconds(2))
2秒执行一次，执行的数据是10秒的数据  
