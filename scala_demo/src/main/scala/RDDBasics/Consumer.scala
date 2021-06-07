package RDDBasics
import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConverters._


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
object Consumer {
   def main(args: Array[String]): Unit = {
    consumeFromKafka("TestTopic")
  }

  def consumeFromKafka(topic: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    while (true) {
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator)
        println(data.value())
       
        
    
    }
    
  /* val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "192.168.1.100:9092")
        .option("subscribe", "TestTopic")
       // .option("startingOffsets", "earliest") // From starting
        .load()

       df.printSchema()
    
        val schema = new StructType()
           .add("id",IntegerType)
           .add("firstname",StringType)
           .add("middlename",StringType)
           .add("lastname",StringType)
           .add("dob_year",IntegerType)
           .add("dob_month",IntegerType)
           .add("gender",StringType)
           .add("salary",IntegerType)
     
         print(schema)*/
   }
}