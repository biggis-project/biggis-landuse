package org.apache.spark.mllib.classification

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import java.io.{ObjectInputStream, ObjectOutputStream}
import org.apache.spark.mllib.util.{DataValidators, Loader, Saveable}

import org.apache.spark.mllib.classification.impl.GLMClassificationMultiClassOVAModel

class SVMMultiClassOVAModel(classModels: Array[SVMModel]) extends ClassificationModel with Serializable with Saveable {

  val classModelsWithIndex : Array[(SVMModel, Int)] = classModels.zipWithIndex

  /**
   * Predict values for the given data set using the model trained.
   *
   * @param testData RDD representing data points to be predicted
   * @return an RDD[Double] where each entry contains the corresponding prediction
   */
  override def predict(testData: RDD[Vector]): RDD[Double] = {
    val localClassModelsWithIndex = classModelsWithIndex
    val bcClassModels = testData.context.broadcast(localClassModelsWithIndex)
    testData.mapPartitions { iter =>
      val w = bcClassModels.value
      iter.map(v => predictPoint(v, w))
    }
  }

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param testData array representing a single data point
   * @return predicted category from the trained model
   */
  override def predict(testData: Vector): Double = predictPoint(testData, classModelsWithIndex)

  def predictPoint(testData: Vector, models: Array[(SVMModel, Int)]): Double =
    models
      .map { case (classModel, classNumber) => (classModel.predict(testData), classNumber)}
      .maxBy { case (score, classNumber) => score}
      ._2

  override protected def formatVersion: String = "1.0"

  override def save(sc: SparkContext, path: String): Unit = {
    //GLMClassificationModel.SaveLoadV1_0(sc, path, this.getClass.getName,)
    GLMClassificationMultiClassOVAModel.SaveLoadV1_0.save(sc, path, this.getClass.getName, classModelsWithIndex)
    /*
    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
    val file = hdfs.create(new org.apache.hadoop.fs.Path(path))
    val out = new ObjectOutputStream(file)
    out.writeObject(classModelsWithIndex)
    file.close()
    // */
  }
}

object SVMMultiClassOVAModel /*extends Loader[SVMMultiClassOVAModel]*/{

  def load(sc: SparkContext, path: String): SVMMultiClassOVAModel = {
    //*
    val (loadedClassName, version, metadata) = Loader.loadMetadata(sc, path)
    val classNameV1_0 = "org.apache.spark.mllib.classification.SVMMultiClassOVAModel"
    (loadedClassName, version) match {
      case (className, "1.0") if className == classNameV1_0 =>
        val data = GLMClassificationMultiClassOVAModel.SaveLoadV1_0.loadData(sc, path, this.getClass.getName)
        val dataModels = data.classModelsWithIndex.map( item => item._1 )
        val model = new SVMMultiClassOVAModel(dataModels)
        model
      case _ => throw new Exception(
        s"SVMMultiClassOVAModel.load did not recognize model with (className, format version):" +
          s"($loadedClassName, $version).  Supported:\n" +
          s"  ($classNameV1_0, 1.0)")
    }
    // */
    /*
    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
    val file = hdfs.open(new org.apache.hadoop.fs.Path(path))
    val in = new ObjectInputStream(file)
    val input = in.readObject()
    file.close()
    val localClassModelsWithIndex : Array[(SVMModel, Int)] = input.asInstanceOf[Array[(SVMModel, Int)]]
    //val localClassModels : Array[SVMModel] = input.asInstanceOf[Array[SVMModel]]
    val localClassModels : Array[SVMModel] = localClassModelsWithIndex.map( item => item._1 )
    //classModelsWithIndex = localClassModelsWithIndex
    val model = new SVMMultiClassOVAModel(localClassModels)
    model
    // */
  }
}


object SVMMultiClassOVAWithSGD {

  /**
   * Train a Multiclass SVM model given an RDD of (label, features) pairs,
   * using One-vs-Rest method - create one SVMModel per class with SVMWithSGD.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param regParam Regularization parameter.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   */
  def train(
             input: RDD[LabeledPoint],
             numIterations: Int,
             stepSize: Double,
             regParam: Double,
             miniBatchFraction: Double): SVMMultiClassOVAModel = {

    val numClasses = input.map(_.label).max().toInt

    val classModels = (0 until numClasses).map { classId =>

      val inputProjection = input.map { case LabeledPoint(label, features) =>
        LabeledPoint(if (label == classId) 1.0 else 0.0, features)}.cache()
      val model = SVMWithSGD.train(inputProjection, numIterations, stepSize, regParam, miniBatchFraction)
      inputProjection.unpersist(false)

      model.clearThreshold()
      model

    }.toArray

    new SVMMultiClassOVAModel(classModels)

  }

  /**
   * Train a Multiclass SVM model given an RDD of (label, features) pairs,
   * using One-vs-Rest method - create one SVMModel per class with SVMWithSGD.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param stepSize Step size to be used for each iteration of Gradient Descent.
   * @param regParam Regularization parameter.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a SVMModel which has the weights and offset from training.
   */
  def train(input: RDD[LabeledPoint], numIterations: Int, stepSize: Double, regParam: Double): SVMMultiClassOVAModel =
    train(input, numIterations, stepSize, regParam, 1.0)

  /**
   * Train a Multiclass SVM model given an RDD of (label, features) pairs,
   * using One-vs-Rest method - create one SVMModel per class with SVMWithSGD.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a SVMModel which has the weights and offset from training.
   */
  def train(input: RDD[LabeledPoint], numIterations: Int): SVMMultiClassOVAModel = train(input, numIterations, 1.0, 0.01, 1.0)

}
