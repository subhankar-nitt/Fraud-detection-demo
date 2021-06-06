package com.example.Producer

import org.apache.kafka.clients.producer.Producer
import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import java.nio.file.Files
import java.nio.file.Paths
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.UUID
import java.io.IOException



class ProducerConf {
  
  var kafkaBrokerPoint : String = "localhost:9092"
  var myTopic : String = "myJava"
  var csvFile : String = "C:/Users/user/Downloads/transactions.csv"
  
  val kafkaProducerProps:Producer[String,String] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    
    new KafkaProducer[String,String](props)    
  }
  
  def PublishMessage(){
    val producer : Producer[String,String] = kafkaProducerProps
    try{
      
    	val fileStream = Files.lines(Paths.get(csvFile))
    	println(fileStream)
    	fileStream.forEach(stream=>{
    	  val csvRecord = new ProducerRecord[String,String](myTopic,UUID.randomUUID().toString(),stream)
    	  
    	  producer.send(csvRecord,(metadata,exception)=>{
    	    if(metadata!=null){
    	      println(f"csvData -> ${csvRecord.key()}  ${csvRecord.value()}")
    	    }else{
    	      println("some exception happened ${exception}")
    	    }
    	  });
    	});
    	
    	
    	}catch{
    	  case e: Exception => println("Error happende"+ e)
    }
    
    
  }
}