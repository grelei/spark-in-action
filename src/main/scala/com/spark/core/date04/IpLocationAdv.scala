package com.spark.core.date04

import com.spark.util.IpUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object IpLocationAdv {
  def main(args: Array[String]): Unit = {
    val isLocal = args(0).toBoolean
    //创建spark上下文
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    if(isLocal){
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)
    //读取IP规则数据，然后collect到Driver端
    val ipLines = sc.textFile(args(1))
    //对IP规则数据进行整理
    val ipRulesRdd:RDD[(Long,Long,String,String)] = ipLines.map( line => {
      val fields = line.split( "[|]" )
      val startNum = fields( 2 ).toLong
      val endNum = fields( 3 ).toLong
      val province = fields( 6 )
      val city = fields( 7 )
      (startNum, endNum, province, city)
    } )
    //将全部的IP规则数据搜集到Driver端
    val ipRoulesInExecutor:Array[(Long,Long,String,String)] = ipRulesRdd.collect()
    //将Driver端的全部IP规则再广播到Executor
     //广播完成之后，将Executor端广播的数据引用返回，以后可以通过这个引用获取到事先广播好的数据
     //sc.broadcast是一个阻塞的方法，广播变量没广播完，不会执行下面逻辑
     val ipRulesRef = sc.broadcast(ipRoulesInExecutor)

    //处理IP日志数据
    val logRDD: RDD[String] = sc.textFile(args(2))
    //整理数据
     //闭包：函数内部引用了一个函数外部得变量，例： ipRulesRef
    val ProvinceAndOne = logRDD.map( line => {
      val fields = line.split( "[|]" )
      val ip = fields( 1 )
      val ipNum = IpUtil.ip2Long( ip )
      //获取实现广播到Executor的数据呢？通过Driver端的引用获取Executor广播好的数据
      val ipRulesInExecutor = ipRulesRef.value
      //调用二分法查找
      val index = IpUtil.binarySearch( ipRulesInExecutor, ipNum )
      var province = "未知"
      if (index >= 0) {
        province = ipRulesInExecutor( index )._3

      }
      (province, 1)
    } )
    //按照省份进行聚合
    val result: RDD[(String, Int)] = ProvinceAndOne.reduceByKey(_+_)
    //打印一下
    //print(result.collect().toBuffer)
    result.saveAsTextFile(args(3))
    sc.stop()



  }
}
