# flink框架scala版入门程序

最佳flink-scala工程创建实践

## 首次工程创建

1、创建主工程，用Maven创建    

2、如果安装最新的flink-1.12.0版本，但是这个有两个版本，分别是scala-2.11和scala-2.12，下载哪个呢？

基于flink-1.12.0搭建scala版本程序
```bash
mvn archetype:generate                               \
  -DarchetypeGroupId=org.apache.flink              \
  -DarchetypeArtifactId=flink-quickstart-scala      \
  -DarchetypeVersion=1.12.0
```

以上命令会生成一个pom文件，查看pom：
```xml
<properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <flink.version>1.12.0</flink.version>
    <scala.binary.version>2.11</scala.binary.version>
    <scala.version>2.11.12</scala.version>
    <log4j.version>2.12.1</log4j.version>
</properties>
```

flink-1.12.0版本配合scala是2.11.12，故下载flink-1.12.0-bin-scala_2.11.tgz进行安装

3、同时IDEA-File-Project Structure-Global Libraries中选择scala-sdk-2.11.12进行安装
   只有按这种方式创建的工程编译才不会出错

4、设置完后再创建子工程，用Maven创建
   需要把mvn生成的工程移到这个工程下面来  
5、在Run/Debug Configuation里面，把Include Dependencies with Provided scope打上勾

## 以后工程创建

1、创建主工程，用Maven创建
2、用Maven创建子工程
3、pom文件拷贝，maven更新
4、在Run/Debug Configuation里面，把Include Dependencies with Provided scope打上勾