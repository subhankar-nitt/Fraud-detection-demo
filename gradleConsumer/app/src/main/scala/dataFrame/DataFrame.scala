package dataFrame

import scala.collection.mutable.ArrayBuffer

class DataFrame {

  val array = new ArrayBuffer[String]()
  

  def addToArray(data:String): Unit ={
     data.split(",").foreach(println)
  }

}
