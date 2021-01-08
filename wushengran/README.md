# 尚硅谷武晟然视频练习

https://www.bilibili.com/video/BV1Qp4y1Y7YN?p=21
练习使用了最新的flink 1.12.0，视频是1.10

## 增加kafka依赖

https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka_2.12/1.12.0  
得知和flink 1.12.0搭配的kafka是2.12

## kafka测试

测试SourceTest
```bash
cd zookeeper/conf
cp zoo_sample.cfg zoo.cfg
cd ..
./bin/zkServer.sh start

cd kafka
./bin/kafka-server-start.sh -daemon ./config/server.properties

./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sensor
sensor_1, 1547718199, 35.8
```

测试KafkaSinkTest
```bash
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sinktest
```
再启动程序  

测试KafkaSinkTest2，这个是kafka作为数据管道测试  
```bash
./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sensor  
sensor_6,1547718201,15.4

./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sinktest

启动程序
```
## reduce

第一个参数：上一次累积的结果  
第二个参数：这一次进来的数据

## 1.12版本已经使用SideOutPut替换split实现分流

## 区别

Union必须是两个相同类型流
connect可以是不同类型流，再加上CoMap转换回DataStream

## 测试redis

brew services start redis  
keys *
跑程序  
keys *  
hgetall sensor_temp  

## 不知道如何使用es sink？

看源码，看到构造方法是私有，就找有没有build方法

## 找最新es包

搜索flink-connector-elasticsearch7

## 测试es

从pom包得知es是7.2.11版本最新  
https://www.elastic.co/cn/downloads/past-releases#elasticsearch  
得知es匹配的版本是7.2.1，下载mac版    

./bin/elasticsearch
跑程序
curl localhost:9200/_cat/indices?v  
curl localhost:9200/sensor/_search?pretty  

## mysql

首先查看服务端mysql版本，是8.0.16  
mvnrepositry中找对应版本的pom库  

create table sensor_temp (
    id varchar(20) not null,
    temp double not null
)

## windowTest碰到问题

【ok】版本原因，无法运行，报：
did you forget to call 'DataStream.assignTimestampsAndWatermarks'
设置assignTimestampsAndWatermarks后ok

## 延时时间测试 WindowTest2

程序逻辑是，15秒一个窗口，watermark超时时间3秒，允许1分钟延迟  

由于不知道确切窗口起止时间，就尝试，先分别输入：
sensor_1,1547718199,35.8
sensor_6,1547718201,15.4
sensor_7,1547718202,6.7
sensor_10,1547718205,38.1
sensor_1,1547718206,32
sensor_1,1547718208,36.2
sensor_1,1547718210,29.7
sensor_1,1547718213,30.9
直到输入最后一条，才输出：
result> (sensor_1,32.0,1547718208)
result> (sensor_7,6.7,1547718202)
result> (sensor_6,15.4,1547718201)
result> (sensor_10,38.1,1547718205)
根据左闭右开，watermark = 13 - 3 = 10，10没在里面，说明10是分界  
得出当前窗口是[195-210) 下一个窗口应该是 [210 - 225) 
故输入：
sensor_1,1547718212,28.1
sensor_1,1547718225,29 应该不会输出，
而输入：
sensor_1,1547718228,32.3 减3到25了触发 10-25窗口
会输出：result> (sensor_1,28.1,1547718212)

测迟到数据（1分钟以内，来一条输出一条）：
这时输入迟到数据： sensor_1,1547718213,19.5
会直接输出： result> (sensor_1,19.5,1547718213)
输入 sensor_1,1547718215,27.6
输出：result> (sensor_1,19.5,1547718215)

重复以上规律：
输入：sensor_1,1547718285,28   触发以上所有数据
输出 result> (sensor_1,29.0,1547718228)     [225-240)这个窗口最小温度是29.0

来迟到数据： sensor_1,1547718218,24.4  
直接输出： result> (sensor_1,19.5,1547718218)
这条数据还能输出，说明之前窗口还没有关闭，说明1分钟延迟是针对watermark的，上面285来了后，watermark为282了，跟上一个225窗口
比起来只过了57秒

如果再来： sensor_1,1547718288,30  则watermark为285，这时225窗口彻底关闭，进入这个窗口的数据再也不会输出  
输入： sensor_1,1547718219,23  就进入到了丢弃数据流里面
输出：late> (sensor_1,23.0,1547718219)

## 以上窗口起始点195的确定

起始点 = timestamp - (timestamp - offset + windowSize) % windowSize
      = 1547718199000 - (1547718199000 - 0 + 15000) % 15000
      = 1547718199000 - 4000
      = 1547718195000
起始窗口就是 [195-210)

## 状态测试 StateTest2

输入sensor_1,1547718199,35.8
由于没有设置初值，初值为0，发生跳变，输出：(sensor_1,0.0,35.8)
输入sensor_1,1547718199,37，不发生跳变
输入sensor_1,1547718199,49，发生跳变，输出： (sensor_1,37.0,49.0)