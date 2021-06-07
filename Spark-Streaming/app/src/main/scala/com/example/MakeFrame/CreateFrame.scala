package com.example.MakeFrame

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
class CreateFrame {
  def createDataFrame(input : String){
    val conf = new SparkConf().setMaster("local[2]").setAppName("Testing App")
    
    val spark =SparkSession.builder().config(conf).getOrCreate()
    
    //val inList = input.split(",").toList
    import spark.sqlContext.implicits._    
    val rdd = spark.sparkContext.parallelize(Seq(input))
    //val copy_rdd = rdd.flatMap(f=>f.split(","))
    val remaining = rdd.zipWithIndex().filter(_._2 > 0).keys.map(v => Row(v.toSeq: _*))
   
    val df = rdd.toDF()
    val df1 = df.select(split(col("value"),",").getItem(0).as("Serial No"),
    split(col("value"),",").getItem(1).as("petal_len"),
    split(col("value"),",").getItem(2).as("petal_width"),
    split(col("value"),",").getItem(3).as("Sepal_len"),
    split(col("value"),",").getItem(4).as("Sepal_width"),
    split(col("value"),",").getItem(5).as("Species")).drop("value")
    df1.show()

  }
}