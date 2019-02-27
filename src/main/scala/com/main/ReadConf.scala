package com.main

import com.conf.Common
import com.utils.TimeUtils
import org.apache.log4j.Logger

/**
  * @ClassName ReadConf
  * @Description TODO
  * @Author zhouyang
  * @Date 2019/2/24 15:21
  * @Version 1.0
  **/
object ReadConf {
  //读配置文件
  @transient
  val logger : Logger = Logger.getLogger(this.getClass)
  //val sparkContext = Common.getSparkContext()

  def main(args: Array[String]): Unit = {
    println("startTime="+TimeUtils.getNowDate())
    //

    println("endTime="+TimeUtils.getNowDate())

  }

}
