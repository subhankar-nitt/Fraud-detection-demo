package consumer

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._
import dataFrame.DataFrame

class ConsumerKafka {
  val dFrame = new DataFrame()
  var kafkaParam:Map[String,Object]=Map[String,Object](
      "bootstrap.servers"->"localhost:9092",
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      "group.id"->"consumer"
      );
  var topics: Array[String] = Array("myJava")
  
  def produceResult(){
    val kafkaConsumer = new KafkaConsumer[String,String](mapAsJavaMap(kafkaParam))
    kafkaConsumer.subscribe(asJavaCollection(topics))
    
    while(true){
      val result = kafkaConsumer.poll(100).asScala

      for(data<-result.iterator){

         dFrame.addToArray(data.value().toString())

      }

    }
  }
}