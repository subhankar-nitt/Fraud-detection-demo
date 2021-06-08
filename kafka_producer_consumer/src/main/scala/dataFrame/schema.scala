package dataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType,StructField}
//will make schema here and later do function calling from here
class schema {
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

}
