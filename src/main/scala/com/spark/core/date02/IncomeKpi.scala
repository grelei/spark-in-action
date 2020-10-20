package com.spark.core.date02

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object IncomeKpi {
  private val logger: Logger = LoggerFactory.getLogger(IncomeKpi.getClass)
  def main(args: Array[String]): Unit = {
    val isLocal = args(0).toBoolean
    //创建spark上下文
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    if(isLocal){
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)
    //创建RDD
    val lines = sc.textFile(args(1))
    //解析JSON
    val cidAndMoney :RDD[(Int,Double)] = lines.map( line=>{
      var tp = (-1,0.0)
      //使用FastJson解析数据
      var jsonObj: JSONObject = null
      try{
        jsonObj = JSON.parseObject(line)
        val cid = jsonObj.getInteger("cid").toInt
        val money = jsonObj.getDouble("money").toDouble
        tp = (cid,money)
      }catch {
        case e :JSONException =>{
          //有问题的数据单独处理
          logger.error("Parse json error:=>"+line)
        }
      }

      //获得json中的数据
      //val oid = jsonObj.getString("oid")
        tp
    })
    val result = cidAndMoney.collect()
    print(result.toBuffer)

    sc.stop()


  }
}
