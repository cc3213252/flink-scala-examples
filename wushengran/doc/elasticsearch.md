## 输出到es需要引入json

```xml
<!-- https://mvnrepository.com/artifact/org.apache.flink/flink-json -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-json</artifactId>
    <version>1.12.0</version>
    <scope>provided</scope>
</dependency>
```

## 测试ESoutputTest

./bin/elasticsearch  
curl localhost:9200/_cat/indices?v   
curl localhost:9200/sensor-agg/_search?pretty  

## 【ok】NoClassDefFoundError: org/apache/flink/table/typeutils/TypeCheckUtils

https://blog.csdn.net/weixin_41608066/article/details/106010473  
需要引入  
```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner_${scala.binary.version}</artifactId>
    <version>1.12.0</version>
</dependency>
```

## es支持的模式

append  
upsert
不支持Retract    