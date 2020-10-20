package com.spark.core.date02

/*
 *实现了序列化接口
 * apply 是个相应的属性赋值
 */
case class CaseOrderBean(oid:String,
                         cid:Int,
                         money:Double,
                         longitude:Double,
                         latitude:Double,
                         var categoryName:String,
                         var province:String,
                         var city:String
                        )
