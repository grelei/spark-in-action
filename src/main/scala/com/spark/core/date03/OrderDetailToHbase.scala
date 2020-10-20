package com.spark.core.date03

import java.sql.{Connection, Date, DriverManager, PreparedStatement, ResultSet, SQLException}
import java.util

import com.alibaba.fastjson.{JSON, JSONException}
import com.spark.core.date02.{CaseOrderBean, IncomeKpi}
import com.spark.util.HBaseUtil
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

object OrderDetailToHbase {
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
    val beanRDD = lines.map( line => {
      var bean: CaseOrderBean = null
      try {
        bean = JSON.parseObject( line, classOf[CaseOrderBean] )
      } catch {
        case e: JSONException => {
          //单独处理
          logger.error("Parse json error:=>"+line)
        }
      }
      bean
    } )

    //过滤有问题的数据
    val filterRDD:RDD[CaseOrderBean] = beanRDD.filter(_!= null)
    //查询MySQL关联维度信息
     //使用分区创建一个数据库链接，再使用这个链接查询维度信息
     val result:RDD[CaseOrderBean] = filterRDD.mapPartitions(it =>{
       //分区的数据不为空
       if(it.nonEmpty) {
         //创建一个Mysql的连接Connection
         val conn: Connection = DriverManager.getConnection( "jdbc:mysql://192.168.87.162:3306/ihospital_user?characterEncoding=UTF-8", "root", "123456" )
         //创建一个state
         val statement: PreparedStatement = conn.prepareStatement( "select name from t_category where id = ? " )
         //设置参数
           it.map( bean => {
           statement.setInt( 1, bean.cid )
           //执行
           val resultSet = statement.executeQuery()
           //获取resultSet结果中的数据
           var name: String = null
           while (resultSet.next()) {
             name = resultSet.getString( "name" )
           }
           bean.categoryName = name
           //要进行判断，如果迭代器中已经没有数据了，关闭链接
              if(resultSet!=null){
               resultSet.close()
             }
           if(!it.hasNext){

             if(statement!=null){
               statement.close()
             }
             if(conn!=null){
               conn.close()
             }
           }
           bean
         } )
        }else{
         it
       }
    })
//    var r = result.collect()
//    print(r.toBuffer)
    //将数据保存到Hbase中
    result.foreachPartition(it=>{
      //创建一个Hbase链接
      val connection = HBaseUtil.getConnection("zuolei00,zuolei01,zuolei02",2181)
      val table = connection.getTable(TableName.valueOf("doitorder"))
      val puts = new util.ArrayList[Put](100)
      //遍历迭代器中的数据
      it.foreach(bean =>{

        //设置参数，rorkey
        val put = new Put(Bytes.toBytes(bean.oid))
        //设置列族的数据
        put.addColumn(Bytes.toBytes("order_info"),Bytes.toBytes("category_name"),Bytes.toBytes(bean.categoryName))
        put.addColumn(Bytes.toBytes("other_info"),Bytes.toBytes("money"),Bytes.toBytes(bean.money))
        //将put放入到puts这个list中
        puts.add(put)
        if(puts.size() == 100 ){
          //将数据写入到Hbase
          table.put(puts)
          //清空puts集合中的数据
          puts.clear()
        }
      })
      //将没有达到100条数据的集合也写入到Hbase中
      table.put(puts)
      //关闭Hbase链接
      connection.close()
    })
    sc.stop()
  }
}
