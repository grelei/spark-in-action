package com.spark.core.date01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 *MapPartitionsWithIndexDemo 就是将RDD的每一个分区遍历出来应为外部传入的函数，输入的是迭代器，返回的也是迭代器
 * 可以将分区编号也取出来
 */
object MapPartitionsWithIndexDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MapPartitionsWithIndexDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //parallelize = makeRDD
    //sc.parallelize(List(1,2,3,4,5,6,7))
    val rdd1:RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9),3)

    // MapPartitionsWithIndexDemo方法，将RDD要计算的数据以一个分区的形式遍历出来，应用外部传入的函数
    //一个分区会对应多条数据，一个分区就是一个迭代器
    //并且可以取出这个分区对应的编号
    rdd1.mapPartitionsWithIndex((index,it) => {
      it.foreach(e=>println(index+","+e))
      it.map(_*10)
    }).count()
    //val rdd3 = rdd1.mapPartitions(it => it.map(i=>i*10))
   //val rdd4 = rdd1.mapPartitions(it=>it.filter(i=> i%2 == 0))
    sc.stop()

  }

}
