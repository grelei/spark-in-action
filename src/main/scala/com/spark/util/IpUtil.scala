package com.spark.util

import scala.collection.mutable.ArrayBuffer

object IpUtil {
  /*
   *将IP地址转成十进制
   * @param ip
   * @return
   */
  def ip2Long(ip:String):Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for(i<- 0 until fragments.length){
      ipNum = fragments(1).toLong | ipNum << 8L
    }
    ipNum
  }
  /*
   * 二分法查找
   * @param lines
   * @param ip
   * @return
   */
  def binarySearch(lines:ArrayBuffer[(Long,Long,String,String)],ip:Long):Int={
    var low = 0 //起始
    var higt = lines.length -1 //结束
    while(low <= higt){
      val middle = (low+higt)/2
      if((ip>=lines(middle)._1) && (ip <= lines(middle)._2)){     //lines(middle)._1 表示起始值 lines(middle)._2 表示结束值
        return middle
      }
      if(ip<lines(middle)._1){
        higt = middle - 1
      }else{
        low = middle + 1
      }
    }
    -1 //没有找到
  }
  def binarySearch(lines:Array[(Long,Long,String,String)],ip:Long):Int={
    var low = 0 //起始
    var higt = lines.length -1 //结束
    while(low <= higt){
      val middle = (low+higt)/2
      if((ip>=lines(middle)._1) && (ip <= lines(middle)._2)){     //lines(middle)._1 表示起始值 lines(middle)._2 表示结束值
        return middle
      }
      if(ip<lines(middle)._1){
        higt = middle - 1
      }else{
        low = middle + 1
      }
    }
    -1 //没有找到
  }
}
