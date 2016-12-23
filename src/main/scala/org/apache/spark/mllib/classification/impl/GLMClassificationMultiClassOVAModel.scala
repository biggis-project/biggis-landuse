package org.apache.spark.mllib.classification.impl

import org.apache.spark.mllib.classification.impl.GLMClassificationModel
import org.apache.spark.mllib.classification.impl.GLMClassificationModel.SaveLoadV1_0

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.util.{DataValidators, Loader, Saveable}
import org.apache.spark.sql.{Row, SQLContext}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

private[classification] object GLMClassificationMultiClassOVAModel {
  object SaveLoadV1_0 {
    def thisFormatVersion: String = "1.0"
    case class Data(classModelsWithIndex: Array[(SVMModel, Int)])
    def save( sc: SparkContext, path: String, modelClass: String, dataModels: Array[(SVMModel, Int)]): Unit = {
      //val modelClass: String = modelClass //"org.apache.spark.mllib.classification.SVMMultiClassOVAModel"
      val numFeatures: Int = dataModels.reduceLeft ((x, y) => if (x._1.weights.size > y._1.weights.size) x else y)._1.weights.size
      val numClasses: Int = dataModels.length
      //val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val sqlContext = SQLContext.getOrCreate (sc)
      import sqlContext.implicits._
      val metadata = compact (render (
        ("class" -> modelClass) ~ ("version" -> thisFormatVersion) ~
          ("numFeatures" -> numFeatures) ~ ("numClasses" -> numClasses) ) )
      sc.parallelize (Seq (metadata), 1).saveAsTextFile (Loader.metadataPath (path) )
      val data = Data (dataModels)
      //spark.createDataFrame(Seq(data)).repartition(1).write.parquet(Loader.dataPath(path))
      sc.parallelize (Seq (data), 1).toDF ().write.parquet (Loader.dataPath (path))
    }
    def loadData(sc: SparkContext, path: String, modelClass: String): Data = {
      val dataPath = Loader.dataPath(path)
      //val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      //val dataRDD = spark.read.parquet(dataPath)
      val dataRDD = sqlContext.read.parquet(dataPath)
      val dataArray = dataRDD.select("dataModels").take(1)
      assert(dataArray.length == 1, s"Unable to load $modelClass data from: $dataPath")
      val data = dataArray(0)
      assert(data.size == 1, s"Unable to load $modelClass data from: $dataPath")
      val(dataModels) = data match {case Row(dataModels: Array[(SVMModel, Int)]) => dataModels}
      Data(dataModels)
    }
  }
}