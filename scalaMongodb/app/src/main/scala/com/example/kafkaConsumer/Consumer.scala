package com.example.kafkaConsumer

import org.apache.kafka.common.serialization.StringDeserializer
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConverters.asJavaCollection
import org.apache.kafka.clients.consumer.KafkaConsumer

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._
import com.mongodb.client.MongoDatabase
import org.bson.Document
import org.bson.types.ObjectId
import com.example.data.Data


class Consumer {
  var mongo = new Data()
  var kafkaParam:Map[String,Object]=Map[String,Object](
      "bootstrap.servers"->"localhost:9092",
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      "group.id"->"consumer"
      );
  var topics: Array[String] = Array("myJava")
  
  def produceResult(db:MongoDatabase,collectionName:String){
    //val collection = db.getCollection(collectionName)
    val kafkaConsumer = new KafkaConsumer[String,String](mapAsJavaMap(kafkaParam))
    kafkaConsumer.subscribe(asJavaCollection(topics))
    
    while(true){
      val result = kafkaConsumer.poll(100).asScala

      for(data<-result.iterator){
        var doc :Document = new Document("_id",new ObjectId())
        val array:Array[String]=data.value().toString().split(",")
        for(a<- 0 to array.length-1){
          val name: String= "colNO "+a 
          doc.append(name,array(a))
        }
        mongo.insertData(db, collectionName, doc)
      }

    }
  }
}