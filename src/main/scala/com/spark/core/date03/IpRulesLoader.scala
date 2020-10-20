package com.spark.core.date03

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

object IpRulesLoader {
  //在Object中定义的数据是静态的，在JVM进程中，只有一份
  var ipRules = new ArrayBuffer[(Long,Long,String,String)]()

  //加载IP规则数据，在Executor的类加载时执行一次
   //静态代码块
  {
    //读取HDFS中的数据
    val fileSystem = FileSystem.get(new URI("hdfs://zuolei00:8020/ip/ip.txt"),new Configuration())
    val inputStream = fileSystem.open(new Path("/ip/ip.txt"))
    val bufferedReader = new BufferedReader(new InputStreamReader(inputStream))
    //读取数据
    var line :String = null
    do{
      line = bufferedReader.readLine()
      if(line != null){
        //处理IP规则数据
        val fields = line.split("[|]")
        val startNum = fields(2).toLong
        val endNum = fields(3).toLong
        val province = fields(6)
        val city = fields(7)
        val t = (startNum,endNum,province,city)
        ipRules += t
      }
    }while(line!=null)

  }
  def getAllRules(): ArrayBuffer[(Long, Long, String, String)] = {
    ipRules
  }
}
