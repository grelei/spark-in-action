package com.spark.test

import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCond {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ScalaWordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C:\\Users\\grele\\Desktop\\TC\\tmp1",1)
    val words = lines.flatMap(line => line.split("\t"))
    val pairs = words.map(word=>(word,1))
    val wordConds = pairs.reduceByKey(_+_)
    wordConds.foreach(wordCond =>println("单词："+wordCond._1+"出现了"+wordCond._2+"次."))
  }
}
