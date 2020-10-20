package com.spark.core.date01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 *就是将RDD的每一个分区拿出来，作为外部传入的函数
 */
object MapPartitionsDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MapPartitionsDemo").setMaster("local")
    val sc = new SparkContext(conf)
    //parallelize = makeRDD
    //sc.parallelize(List(1,2,3,4,5,6,7))
    val rdd1:RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9),3)
    //map方法是将RDD要计算的数据一条一条的遍历出来应用外部传入的函数
    val rdd2 = rdd1.map(i=>i*10)
    rdd2.foreach(i=>println(i))
    //maoPartitions方法，将RDD要计算的数据以一个分区的形式遍历出来，应用外部传入的函数
    //一个分区会对应多条数据，一个分区就是一个迭代器
    //val rdd3 = rdd1.mapPartitions(it => it.map(i=>i*10))
   //val rdd4 = rdd1.mapPartitions(it=>it.filter(i=> i%2 == 0))

  }

}
