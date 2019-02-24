package com.utils

import java.text.SimpleDateFormat
import java.util.Date

object TimeUtils {

  /*获取系统时间*/
  def getNowDate():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS")
    var hehe = dateFormat.format(now)
    hehe
  }

  def getNowDate2():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    var hehe = dateFormat.format(now)
    hehe
  }

  /**
    * Description: 将yyyy-MM-dd HH:mm:ss 格式字符串转成yyyyMMddHHmmss
    * Author: zhouyang
    * Date 2018/1/3 20:31
    * @param strTime : String
    *
    * @return
   */
  def getStringDateFormat(strTime : String) : String={
    var  dateFormat1:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateFormat2:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    import java.text.ParsePosition
    val pos = new ParsePosition(0)
    val strtodate = dateFormat1.parse(strTime, pos)
    val mytime = dateFormat2.format(strtodate)
    mytime
  }

  def main(args:Array[String]): Unit =
  {
    println(getNowDate2())
    println(getStringDateFormat("2016-6-1 13:17:00"))
    println(getStringDateFormat("2016-6-1 8:17:00").substring(2,12))
  }
}
