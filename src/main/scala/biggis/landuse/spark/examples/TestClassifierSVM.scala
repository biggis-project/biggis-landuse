package biggis.landuse.spark.examples


import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}


object TestClassifierSVM extends StrictLogging {
  /**
    * Run as: /path/to/sample_libsvm_data.txt /path/to/myModel
    * You can download the dataset from:
    * - https://raw.githubusercontent.com/apache/spark/master/data/mllib/sample_libsvm_data.txt
    */
  def main(args: Array[String]): Unit = {
    val Array(trainingName, modelPath) = args
    TestClassifierSVM(trainingName)(modelPath)
  }

  def apply(trainingName: String)(implicit modelPath: String): Unit = {
    logger info s"(SVM) Classifying layer $trainingName in $modelPath ..."
    //ClassifierSVM


    val conf = new SparkConf()
      .setAppName(s"TestClassifierSVM with $trainingName $modelPath")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, trainingName)

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    logger info "Area under ROC = " + auROC

    // Save and load model
    model.save(sc, modelPath)
    val sameModel = SVMModel.load(sc, modelPath)


    //ClassifierSVM
    logger info "done"
  }
}
