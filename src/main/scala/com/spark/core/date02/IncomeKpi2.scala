package com.spark.core.date02

import java.sql.{Connection, Date, DriverManager, PreparedStatement, SQLException}

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import com.spark.core.date02.IncomeKpi.logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object IncomeKpi2 {
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

//    val result = filterRDD.collect()
//    print(result.toBuffer)
    //将数据转成元组，分组聚合
    val cidAndMoney = filterRDD.map( bean => {
      val cid = bean.cid
      val money = bean.money
      (cid, money)
    } )
    //分组聚合之后的RDD
    val reduce:RDD[(Int,Double)] = cidAndMoney.reduceByKey(_+_)
    //再创建一个RDD
    val categoryLine:RDD[String] = sc.textFile(args(2))
    //分类RDD
    val cidAndCname = categoryLine.map( line => {
      val fields = line.split( "," )
      val cid = fields( 0 ).toInt
      val cname = fields( 1 )
      (cid, cname)
    } )
    val joined:RDD[(Int,(Double,String))] = reduce.join(cidAndCname)
    //将join后的数据进行处理
    val result = joined.map(t=>(t._2._2,t._2._1))

    //print(result.collect().toBuffer)
    //将计算好的数据保存到mysql中
     //Action
    //方式一：将数据collect到Drive端再写入到数据库（数据量太多就不行了，因为会产生瓶颈，或者会丢失数据或内存溢出）

    //方式二：直接在Executor端写入到数据库，适用数据量较大的场景，并发写入，效率更高
     //如果写入的数据量比较大，一定要使用foreachPartition 方法
    //## 反面教材
//    result.foreach(t=>{
//      //foreach 一次迭代一条，创建一个链接，写入完成后就释放链接
//      //使用foreachPartition 的好处，一个分区对应一个迭代器，一个迭代器有多条数据，一个分区创建一个链接
//      var conn: Connection = null
//      var statement:PreparedStatement = null
//      try {
//        //创建一个Mysql的连接Connection
//        conn = DriverManager.getConnection( "jdbc:mysql://192.168.87.162:3306/ihospital_user?characterEncoding=UTF-8", "root", "123456" )
//        //创建一个state
//        statement = conn.prepareStatement( "INSERT INTO tmp1 values (null,?,?,?)" )
//        //设置参数
//        statement.setString( 1, t._1 )
//        statement.setDouble( 2, t._2 )
//        statement.setDate( 3, new Date(System.currentTimeMillis()))
//        //执行
//        statement.executeUpdate()
//      }catch{
//        case e:SQLException =>{
//          //将问题数据单独处理
//        }
//      }finally {
//        //释放资源
//        if(statement != null){
//          statement.close()
//        }
//        if(conn != null){
//          conn.close()
//        }
//      }
//    })
    //正确的方式
    //将数据写入到mysql
    result.foreachPartition(dataToMysql)


    sc.stop()


  }


  val dataToMysql = (it :Iterator[(String,Double)])=>{
          var conn: Connection = null
          var statement:PreparedStatement = null
          try {
            //创建一个Mysql的连接Connection
            conn = DriverManager.getConnection( "jdbc:mysql://192.168.87.162:3306/ihospital_user?characterEncoding=UTF-8", "root", "123456" )
            //创建一个state
            statement = conn.prepareStatement( "INSERT INTO tmp1 values (null,?,?,?)" )
            //设置参数
            it.foreach(t=>{
              statement.setString( 1, t._1 )
              statement.setDouble( 2, t._2 )
              statement.setDate( 3, new Date(System.currentTimeMillis()))
              //执行
              //statement.executeUpdate()
              //批量写入
              statement.addBatch()
            })
            statement.executeBatch()
            ()
          }catch{
            case e:SQLException =>{
              //将问题数据单独处理
            }
          }finally {
            //释放资源
            if(statement != null){
              statement.close()
            }
            if(conn != null){
              conn.close()
            }
          }
  }
}
