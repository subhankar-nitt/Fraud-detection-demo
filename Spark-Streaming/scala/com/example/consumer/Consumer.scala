package com.example.consumer

import org.apache.kafka.common.serialization.StringDeserializer
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConverters.asJavaCollection
import org.apache.kafka.clients.consumer.KafkaConsumer

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._
import com.example.MakeFrame.CreateFrame



class Consumer {

  val frame = new CreateFrame()
  var kafkaParam:Map[String,Object]=Map[String,Object](
      "bootstrap.servers"->"localhost:9092",
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      "group.id"->"consumer"
      );
  var topics: Array[String] = Array("myJava")
  
  def produceResult(){
    //val collection = db.getCollection(collectionName)
    val kafkaConsumer = new KafkaConsumer[String,String](mapAsJavaMap(kafkaParam))
    kafkaConsumer.subscribe(asJavaCollection(topics))
    
    while(true){
      val result = kafkaConsumer.poll(100).asScala

      for(data<-result.iterator){
        frame.createDataFrame(data.value().toString())
      }

    }
  }
}