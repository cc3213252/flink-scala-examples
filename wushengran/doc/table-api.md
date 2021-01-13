## 【ok】FileOutputTest中insertInto已经没有了

用executeInsert

## 测试KafkaPiplineTest

./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sinktest  
./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sensor  
启动程序  
输入：   
sensor_1,1547718199,35.8  