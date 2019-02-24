package com.sparkstreaming

import com.sparkstreaming.KafkaStreaming2.jsonDecode
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferBrokers
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
/*
 * @Author zhouyang
 * @Description TODO 启动sparkstreaming
 * @Date 16:53 2019/2/22
 * @Param 
 * @return 
 **/
object KafkaStreaming {

  //
  private lazy val logger = Logger.getLogger(getClass)
  //数据在hdfs上的路径
  val HDFS_DIR = "/test/test-data/kafka-streaming"

  //
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //对象序列化配置
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //sparkconf
    val sparkConf = new SparkConf().setAppName("KafkaStreaming")
    //每60秒一个批次
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    //Kafka集群使用的zookeeper
    val servers = "hxf:2181,cfg:2181,jqs:2181,jxf:2181,sxtb:2181"
    //
    val params = Map[String, Object](
      "bootstrap.servers" -> servers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "group.id" -> "launcher-streaming",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // 要读的topic name
    val topics = List("launcher_click")

    // 创建kafka流对象
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferBrokers, Subscribe(topics, params)
    )
    // just alias for simplicity
    type Record = ConsumerRecord[String, String]
    //rdd 计算
    stream.foreachRDD((rdd : RDD[Record], time : Time) => {
      rdd.map(row => (row.timestamp(), jsonDecode(row.value())))
    })

    //启动sparkstreaming context，提交任务
    ssc.start()
    // 等待实时流
    ssc.awaitTermination()
  }

}



