package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object  Add_n {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Add_n").setMaster("local")
    val sc = new SparkContext(conf);
    val lines = sc.textFile("C:\\Users\\grele\\Desktop\\new7",1)

    lines.foreach(w => println(w))
  }
  def add(x:String,y:String)=x+y
}
