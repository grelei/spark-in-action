package com.spark.core.date05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CutomStort1 {
  def main(args: Array[String]): Unit = {
    val isLocal = args(0).toBoolean
    //创建spark上下文
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    if(isLocal){
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.parallelize(List("laozhang,30,99.99","lisan,28,99.99","langzhao,89,100"))
    val tfboy = lines.map( line => {
      var fields = line.split( "," )
      val name = fields( 0 )
      val age = fields( 1 ).toInt
      val fv = fields( 2 ).toDouble
      new boy( name, age, fv )
    } )
    val sorted: RDD[boy] = tfboy.sortBy( x=>x)
    print(sorted.collect().toBuffer)
    sc.stop()
  }
}
