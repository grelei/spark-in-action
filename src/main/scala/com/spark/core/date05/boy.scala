package com.spark.core.date05


class boy(var name:String,var age:Int,var fv:Double) extends Comparable[boy] with Serializable {
  override def compareTo(o: boy): Int = {
    if(this.fv == o.fv){
      this.age-o.age

    } else{
      -java.lang.Double.compare(this.fv,o.fv )
    }
  }

  override def toString = s"boy($name, $age, $fv)"
}
