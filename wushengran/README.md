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
 