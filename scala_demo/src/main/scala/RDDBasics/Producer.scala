package RDDBasics
import org.apache.kafka.clients.producer._
import java.util.Properties
import scala.io.Source
//import com.sun.prism.impl.Disposer.Record

object Producer {
   def main(args: Array[String]): Unit = {
    writeToKafka("TestTopic")
  }

  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val fileName="C:/Users/HP/OneDrive/Desktop/Spark/FraudDetection-master/src/main/resources/data/customer.csv"
    for(line<- Source.fromFile(fileName).getLines()){
      val key=line.split(","){0}
      val record = new ProducerRecord[String, String](topic,key,line);
      producer.send(record)
    }
    producer.close()
  }
}