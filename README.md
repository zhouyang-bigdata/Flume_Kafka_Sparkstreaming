# **流式数据解决方案参考框架一**

**一 适用应用场景**

流式数据

低延时

...

**二 架构**

 

**（1）流程图**

![](Flume_Kafka_Sparkstreaming\doc\架构.png)

**（2）处理流程**

Flume会实时监控写入日志的磁盘，只要有新的日志写入，Flume就会将日志以消息的形式传递给Kafka，然后Spark Streaming实时消费消息传入Hive

 

 

**（3）预期效果**

通过这套架构，收集到的日志可以及时被Flume发现传到Kafka，通过Kafka我们可以把日志用到各个地方，同一份日志可以存入Hdfs中，也可以离线进行分析，还可以实时计算，而且可以保证安全性，基本可以达到实时的要求。

 

**（4）效果计算分析**

每秒多少条，一天处理多少数据量，计算时效性

 

**三 部署流程**

硬件环境

至少3台服务器。

软件环境

1. CentOs 7.0

2. Java >= 1.7 (Oracle JDK has been tested)
3. Maven >= 3
4. Apache Spark == 1.6
5. scala >=2.10
6. Kafka >= 0.10.1.0
7. hive >=1.1
8. hadoop >=2.6

 

1. 安装、配置flume

2. 安装、配置kafka

3. 安装、配置zookeeper

4. 安装、配置hadoop

5. 安装、配置hive

6. 安装、配置spark

7. 配置开发环境

   

 

 

**四 运行**

1. 启动kafka，spark，hadoop，hive
2. 编译打包

```
mvn clean package -DskipTests
```

​	启动：

```bash
$ cd kafka-spark-streaming-example
$ java -Dconfig=./config/common.conf -jar spark_streaming/target/spark_streaming-1.0-SNAPSHOT.jar
```



```bash
$ java -Dconfig=./config/common.conf -jar kafka_producer/target/kafka_producer-1.0-SNAPSHOT.jar
```

```
$ java -Dconfig=./config/common.conf -jar hive_analysis/target/hive_analysis-1.0-SNAPSHOT.jar
```



3. 项目结构

![](D:\BIG-DATA\Flume_Kafka_Sparkstreaming\doc\项目目录结构.png)

**五 测试**





