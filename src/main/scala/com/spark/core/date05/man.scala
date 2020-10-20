package com.spark.core.date05

case class man(var name:String, var age:Int, var fv:Double) extends Ordered[man]   {


  override def compare(o: man): Int = {
    if(this.fv == o.fv){
      this.age-o.age

    } else{
      -java.lang.Double.compare(this.fv,o.fv )
    }
  }

  override def toString = s"man($name, $age, $fv)"

}
