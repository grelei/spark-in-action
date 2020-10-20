package com.spark.util



import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory

/*
 *Hbase的工具类，用来创建Hbase的Connection
 */
object HBaseUtil extends Serializable {
  /*
   *@param zkQuorum  --Zookeeper 地址，多个要用逗号分隔
   * @param port --zookeeper 端口号
   * @return
   */
  def getConnection(zkQuorum:String,port:Int) =synchronized {
    val conf = HBaseConfiguration.create()
    conf.set( "Hbase.zookeeper.quorum", zkQuorum )
    conf.set( "Hbase.zookeeper.property.clientPort", port.toString )
     ConnectionFactory.createConnection( conf )

  }

}
