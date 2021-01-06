# 尚硅谷武晟然视频练习

https://www.bilibili.com/video/BV1Qp4y1Y7YN?p=21

## 增加kafka依赖

https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka_2.12/1.12.0  
得知和flink 1.12.0搭配的kafka是2.12

## kafka测试

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

## reduce

第一个参数：上一次累积的结果  
第二个参数：这一次进来的数据

## 1.12版本已经使用SideOutPut替换split实现分流

## 区别

Union必须是两个相同类型流
connect可以是不同类型流，再加上CoMap转换回DataStream

## sink

写文件，