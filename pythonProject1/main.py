from pyspark import SparkContext,SparkConf
from pyspark.ml.classification import LogisticRegression
from pyspark.mllib.classification import SVMWithSGD
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType
from pyspark.ml.feature import VectorAssembler

from pyspark.ml.feature import *
from pyspark.ml import Pipeline
spark = SparkSession.builder.master("local[2]").appName("setting").getOrCreate()



data = spark.read.option("header","true").csv("D:/Iris.csv")
data = data.withColumn("Id", col("Id").cast("int")).withColumn("SepalLengthCm", col("SepalLengthCm").cast("float")).withColumn("SepalWidthCm", col("SepalWidthCm").cast("float")).withColumn("PetalLengthCm", col("PetalLengthCm").cast("float")).withColumn("PetalWidthCm", col("PetalWidthCm").cast("float"))
indexer = [StringIndexer().setInputCol("Species").setOutputCol("label").setHandleInvalid("skip").fit(data)]
#labelDf = indexer.fit(data).transform(data)
#data.show()
assembler = VectorAssembler(inputCols=data.columns[1:4],outputCol="features")
logisticRegression = LogisticRegression().setMaxIter(100).setRegParam(0.001)
pipeline = Pipeline().setStages(indexer+[assembler,logisticRegression])
#labelDf.show()
#output=assembler.transform(data)

#indexer = StringIndexer().setInputCol("Species").setOutputCol("label")
#stages+=[indexer]
#labelDf = indexer.fit(output).transform(output)
#stages+=[labelDf]
#final_set = labelDf.select("features","label")
#stages+=final_set
splits = data.randomSplit([0.7,0.3],10)
train = splits[0]
test = splits[1]
#logisticRegression = LogisticRegression().setMaxIter(100).setRegParam(0.02).setElasticNetParam(0.8).setFeaturesCol("features").setLabelCol("label")
#stages+=[logisticRegression]

logisticRegressionModel = pipeline.fit(data)

# dataTest= spark.createDataFrame(
#   [(0, 18, 1.0, 2.9,3.7],
#   ["id", "hour", "mobile", "userFeatures", "clicked"])
#
# mod.

df = spark.readStream .format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "myJava").option("startngOffsets","earliest").load()
schema= StructType([
  StructField("Id", IntegerType()),
  StructField("PetalLengthCm", FloatType()),
  StructField("PetalWidthCm", FloatType()),
  StructField("SepalLengthCm", FloatType()),
  StructField("SepalWidthCm", FloatType()),
  StructField("Species", StringType())
])
table = df.selectExpr("CAST(value AS STRING) as data").select(from_json(col("data"),schema).alias("data"))
next_table = table.selectExpr("data.Id","data.PetalLengthCm","data.PetalWidthCm","data.SepalLengthCm","data.SepalWidthCm","data.Species")

print(type(next_table))
predictionDf = logisticRegressionModel.transform(next_table)

query2 = predictionDf.writeStream.outputMode("append").option("truncate","false").format("console").start()

query2.awaitTermination()