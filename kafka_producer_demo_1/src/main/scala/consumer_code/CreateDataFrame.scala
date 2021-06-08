package consumer_code
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType,StructField}
object CreateDataFrame {
  def main(args: Array[String]): Unit={
    
    
    val spark = SparkSession.builder()
        .appName(name="Create Dataframe")
        .master(master= "local[*]")
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR");
    
    val kafka_topic_name = "User_data1"
    val kafka_bootstrap_server = "localhost:9092"
    val user_df = spark.read
        .format(source = "kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_server)
        .option("subscribe", kafka_topic_name)
        .load()
        
    
    val users_df_1= user_df.selectExpr(exprs="CAST(value As String)","CAST(timestamp AS TIMESTAMP)")
    println(users_df_1)
    val user_schema = StructType(Array(
        StructField("credit_num",IntegerType,true),
        StructField("First_Name",StringType,true),
        StructField("Last_Name",StringType,true),
        StructField("Trans_num",StringType,true),
        StructField("Amount",IntegerType,true),
        StructField("Category",StringType,true),
        StructField("Merchant",StringType,true)
        
    )
    )
    val users_df_2= users_df_1.select(from_json(col(colName = "value"),user_schema)
        .as(alias="user_detail"), col(colName ="timestamp"))
    print(users_df_2.show())
    val users_df_3= users_df_2.select(col="user_detail.*",cols="timestamp")
    
    users_df_3.printSchema()
    users_df_3.show(numRows =10,truncate =false)
    
     /* var data_Schema:StructType = new StructType()
         .add("credit_card",StringType)
         .add("user_name",StringType)
         .add("user_city",StringType)
     val df1 = user_df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
      .select(from_json($"value", data_Schema).as("data"), $"timestamp")
      .select("data.credit_card", "data.user_name", "timestamp")
      .toDF("credit_card", "user_name", "Timestamp")
         */
    spark.stop()
    println("Application completed")
  }
  
}