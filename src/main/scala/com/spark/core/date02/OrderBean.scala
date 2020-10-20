package com.spark.core.date02

import scala.beans.BeanProperty

class OrderBean extends Serializable {
  @BeanProperty  //生成setter方法
  var cid : Int = _
  @BeanProperty
  var money : Double =_
  @BeanProperty
  var longitude : Double =_
  @BeanProperty
  var latitude : Double =_


  override def toString = s"OrderBean($cid, $money, $longitude, $latitude)"
}
