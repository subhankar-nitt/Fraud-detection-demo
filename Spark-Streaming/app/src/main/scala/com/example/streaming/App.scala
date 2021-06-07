/*
 * This Scala source file was generated by the Gradle 'init' task.
 */
package com.example.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.example.consumer.Consumer



object App {
  def main(args: Array[String]): Unit = {
    
    val consumer = new Consumer()
    consumer.produceResult()
  }

  def greeting(): String = "Hello, world!"
}
