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

## widowTest碰到问题

【ok】版本原因，无法运行，报：
did you forget to call 'DataStream.assignTimestampsAndWatermarks'
设置assignTimestampsAndWatermarks后ok