package com.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

import com.example.Producer.ProducerConf

object Main {
  def main(args:Array[String]){

    val kafkaProducer = new ProducerConf()
    kafkaProducer.PublishMessage()
     }
  
}