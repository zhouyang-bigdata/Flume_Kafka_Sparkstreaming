package com.conf

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

/*
 * @Author zhouyang
 * @Description TODO spark 基本参数配置
 * @Date 11:41 2019/2/22
 * @Param
 * @return
 **/
object Common {
    val config: Config = ConfigFactory.load
    val hdfsNameService: String = config.getString("kxnf.basic.hdfsNameService")
    val zkServer: String = config.getString("kxnf.basic.zkServers")
    val kafkaBrokerList: String = config.getString("kxnf.basic.kafkaBrokerList")
    var executeFlag = true
    var baseDir:String = System.getProperty("user.dir")
    val clientModel: Boolean = config.getBoolean("kxnf.spark.clientModel")
    val clusterModel: Boolean = config.getBoolean("kxnf.spark.clusterModel")
    val testModel : Boolean = config.getBoolean("kxnf.runningModel.testModel")
    val driverHost : String = config.getString("kxnf.spark.driver.host")
    val executorMemory : String =config.getString("kxnf.spark.executor.memory")
    val serializer : String = config.getString("kxnf.spark.serializer")
    val driverMemory : String = config.getString("kxnf.spark.driver.memory")
    val appName : String = config.getString("kxnf.spark.appName")
    val sparkMaster:String = config.getString("kxnf.spark.master")
    var sparkContext:SparkContext = null

  def getSparkContext(unit: Unit):SparkContext = {
    val conf:SparkConf  = new SparkConf()
    if(testModel){
      conf.set("spark.driver.host",driverHost)
    }

    conf.set("spark.executor.memory", executorMemory)
    conf.set("spark.driver.memory", driverMemory)
    conf.setMaster(sparkMaster)
    conf.setAppName(this.appName)
    conf.set("spark.serializer",serializer)
    //如果使用KryoSerializer方式则 需要添加自定义的序列化类
    //conf.registerKryoClasses(Array())
    sparkContext = new SparkContext(conf)
    //引入dataFrame 使用外部数据源接口来自定义CSV输入格式
    sparkContext
  }


    def getConfMap(path: String): java.util.HashMap[String, String] = {
      val map = new java.util.HashMap[String, String]()
      val entry = config.getConfig(path).entrySet().asScala
      for (e <- entry) {
        val value = e.getValue.render()
        if (StringUtils.isNotBlank(value)) map.put(e.getKey, value)
      }
      map
    }

}
