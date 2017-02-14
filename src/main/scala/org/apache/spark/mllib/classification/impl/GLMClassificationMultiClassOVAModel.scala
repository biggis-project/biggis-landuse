package org.apache.spark.mllib.classification.impl

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.util.{DataValidators, Loader, Saveable}
import org.apache.spark.sql.{Row, SQLContext}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable

private[classification] object GLMClassificationMultiClassOVAModel {
  object SaveLoadV1_0 {
    def thisFormatVersion: String = "1.0"
    case class Data(classModelsWithId: Array[(SVMModel, Int)])
    case class MetaDataId(classId: Array[Int])
    def save( sc: SparkContext, path: String, modelClass: String, dataModels: Array[(SVMModel, Int)]): Unit = {
      val numFeatures: Int = dataModels.reduceLeft ((x, y) => if (x._1.weights.size > y._1.weights.size) x else y)._1.weights.size
      val numClasses: Int = dataModels.length
      val sqlContext = SQLContext.getOrCreate (sc)
      import sqlContext.implicits._
      val metadata = compact (render (
        ("class" -> modelClass) ~ ("version" -> thisFormatVersion) ~
          ("numFeatures" -> numFeatures) ~ ("numClasses" -> numClasses) ) )
      sc.parallelize (Seq(metadata), 1).saveAsTextFile (Loader.metadataPath(path) )
      val metaDataId = MetaDataId( dataModels.map( model => model._2 ) )
      sc.parallelize (Seq (metaDataId), 1).toDF ().write.parquet (Loader.dataPath (path))
      for( modelno <- dataModels.indices) {
        val model = dataModels(modelno)._1
        val modelid = dataModels(modelno)._2
        model.save(sc, path + "/class/" + modelid )
      }
    }
    def loadData(sc: SparkContext, path: String, modelClass: String): Data = {
      val dataPath = Loader.dataPath(path)
      val sqlContext = SQLContext.getOrCreate(sc)
      val dataRDD = sqlContext.read.parquet(dataPath)
      val dataArray = dataRDD.select("classId").take(1)
      assert(dataArray.length == 1, s"Unable to load $modelClass data from: $dataPath")
      val data = dataArray(0)
      assert(data.size == 1, s"Unable to load $modelClass data from: $dataPath")
      val classId = data match {case Row (classId: mutable.WrappedArray[Int]) => classId }
      val numClasses = classId.length
      val dataModels : Array[(SVMModel, Int)] = Array.ofDim[(SVMModel, Int)](numClasses)
      for( modelno <- classId.indices) {
        val modelid = classId(modelno)
        val model = SVMModel.load(sc, path + "/class/" + modelid)
        dataModels(modelid) = (model,modelid)
      }
      Data(dataModels)
    }
  }
}