package com.hive

package org.apache.spark.examples.sql.hive

// $example on:spark_hive$
import java.io.File

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


// $example off:spark_hive$

/**
  * @ClassName SparkHiveExample
  * @Description TODO
  * @Author zhouyang
  * @Date 2019/2/24 16:05
  * @Version 1.0
  **/
object SparkHiveExample2 {
  def main(args: Array[String]): Unit = {
//    val hiveMaster = ""
//    val hiveAccountMartTableName = ""
//    val sparkConfig = new SparkConf().setAppName("SparkHiveExample2")
//    val sc: SparkContext = new SparkContext(sparkConfig)
//
//    val hiveContext = new HiveContext(sc)
//
//    val hiveOptions = Map(
//      "hive.table" -> hiveAccountMartTableName,
//      "hive.master" -> hiveMaster
//    )
//
//    hiveContext.read.options(hiveOptions).format("org.hivedb.spark.hive").load.
//      registerTempTable("account_mart_tmp")
//
//
//    println("------------")
//    val values = hiveContext.sql("select account_id, sum(win_count) from account_mart_tmp group by account_id").
//      take(100)
//    println("------------")
//
//    values.foreach(println)
//    println("------------")
//
//    sc.stop()
  }
}
