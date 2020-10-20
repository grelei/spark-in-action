package com.spark.core.date03

import com.spark.util.IpUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object IpLocation {
  def main(args: Array[String]): Unit = {
    val isLocal = args(0).toBoolean
    //创建spark上下文
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    if(isLocal){
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)
    //创建RDD，先读取日志数据
    val lines = sc.textFile(args(1))
    //对数据进行整理
    val provinceAndOne:RDD[(String,Int)] = lines.map(line => {
      val allRules :ArrayBuffer[(Long, Long, String, String)] = IpRulesLoader.getAllRules()
      val fields = line.split( "[|]" )
      //获取IP 地址
      val iP = fields( 1 )
      //将IP地址转成十进制
      val ipNum = IpUtil.ip2Long(iP)
      //使用二分法查找
      val index = IpUtil.binarySearch(allRules,ipNum)
      var province :String= "未知"
      if(index != -1){
        province = allRules(index)._3
      }
      (province,1)
    } )
    //按照省份聚合
    val result = provinceAndOne.reduceByKey(_+_)
    //将计算好的数据保存好Mysql
    print(result.collect().toBuffer)
    sc.stop()

  }
}
