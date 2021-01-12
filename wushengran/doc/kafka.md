测试TableKafka
```bash
cd zookeeper/conf
cp zoo_sample.cfg zoo.cfg
cd ..
./bin/zkServer.sh start

cd kafka
./bin/kafka-server-start.sh -daemon ./config/server.properties

./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sensor
```