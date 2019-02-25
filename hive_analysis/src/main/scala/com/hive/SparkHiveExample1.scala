package com.hive

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ClassName HiveAnalysis
  * @Description TODO 创建hive对象
  * @Author zhouyang
  * @Date 2019/2/24 15:53
  * @Version 1.0
  **/
object SparkHiveExample1 {
  def main(args: Array[String]): Unit = {
    val hiveMaster = ""
    val hiveAccountMartTableName = ""
    val sparkConfig = new SparkConf().setAppName("HiveAnalysis")
    val sc: SparkContext = new SparkContext(sparkConfig)

    val hiveContext = new HiveContext(sc)


    hiveContext.sql("CREATE TABLE IF NOT EXISTS hive_test_table1 (key INT, value STRING)")
    hiveContext.sql("LOAD DATA LOCAL INPATH 'hive_analysis/src/main/resources/kv1.txt' INTO TABLE hive_test_table1")



    println("------------")
    val values = hiveContext.sql("select key, sum(key) from hive_test_table1 group by key").take(100)

    println("------------")
    values.foreach(println)
    println("------------")

    sc.stop()
  }
}
