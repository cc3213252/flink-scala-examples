## 碰到问题

【ok】2020-12-14 10:12:34,593 WARN  org.apache.flink.runtime.taskmanager.Task                    [] - Source: Collection Source -> Flat Map -> Filter -> Map (1/1) (8aca3e5483bd5aa64ebfd0f90ffb2119) switched from RUNNING to FAILED.
java.lang.NoSuchMethodError: scala.Predef$.refArrayOps([Ljava/lang/Object;)Lscala/collection/mutable/ArrayOps;
是工程类库使用问题，按首页工程创建方式进行创建  

## 注意

pom中不需要profiles配置