package com.example.main

import com.example.mlmodel.Logistic_Regression
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.classification.LogisticRegression


object App {
  def main(args: Array[String]): Unit = {
    greeting()
  }

  def greeting()={
    val conf = new SparkConf().setMaster("local[2]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryo.registrationRequired", "false")

    val spark = SparkSession.builder().appName("ml app").config(conf).getOrCreate()

    val df = spark.read.option("header", true).csv("D:/Iris.csv")
    
   val df1= df.withColumn("Id", col("Id").cast("int"))
   val df2 = df1.withColumn("SepalLengthCm", col("SepalLengthCm").cast("float"))
   val df3 =  df2.withColumn("SepalWidthCm", col("SepalWidthCm").cast("float"))
   val df4 = df3.withColumn("PetalLengthCm", col("PetalLengthCm").cast("float"))
   val df5 = df4.withColumn("PetalWidthCm", col("PetalWidthCm").cast("float"))
   
   
   val assembler = new VectorAssembler().setInputCols(Array("SepalLengthCm","SepalWidthCm","PetalLengthCm","PetalWidthCm")).setOutputCol("Attribute")
   val output=assembler.transform(df5)
   output.printSchema()
   val indexer = new StringIndexer()
  .setInputCol("Species")
  .setOutputCol("label")
   val labelDf = indexer.fit(output).transform(output)

  
    val data = labelDf.select("Attribute", "label")
 
val seed = 5043
val splits = labelDf.randomSplit(Array(0.7, 0.3), seed)
val trainingData = splits(0).cache()
val testData = splits(1)
// train logistic regression model with training data set
val logisticRegression = new LogisticRegression()
  .setMaxIter(100)
  .setRegParam(0.02)
  .setElasticNetParam(0.8)
  .setFeaturesCol("Attribute")
  .setLabelCol("label")
  data.printSchema()
val logisticRegressionModel = logisticRegression.fit(trainingData)

// run model with test data set to get predictions
// this will add new columns rawPrediction, probability and prediction
val predictionDf = logisticRegressionModel.transform(testData)
predictionDf.show(10)
  }
}
