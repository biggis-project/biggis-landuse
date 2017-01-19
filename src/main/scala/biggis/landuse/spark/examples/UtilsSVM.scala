package biggis.landuse.spark.examples

import geotrellis.raster.MultibandTile
import geotrellis.raster.Tile
import geotrellis.spark.{SpaceTimeKey, SpatialKey, TemporalKey}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
  * Created by ak on 01.12.2016.
  */
object UtilsSVM {

  case class LabelPointSpatialRef(spatialKey: SpatialKey, offset: Int)

  def MultibandTile2PixelSamples(tile: MultibandTile): Iterable[(Int, Int, List[Double])] = {

    val xy = for (x <- 0 until tile.cols;
                  y <- 0 until tile.rows) yield (x, y)

    xy.map { case (x, y) =>
      val features = for (b <- 0 until tile.bandCount) yield tile.band(b).getDouble(x, y)
      (x, y, features.toList)
    }
  }

  def MultibandTile2LabeledPixelSamples(tile: MultibandTile,
                                        classBandNo: Int): Iterable[(Int, Int, LabeledPoint)] = {

    MultibandTile2PixelSamples(tile).map{case(x, y, features) =>
      val label = features(classBandNo)
      val featuresWithoutLabel = features.take(classBandNo - 1) ::: features.drop(classBandNo)
      val featuresMllib = Vectors.dense(featuresWithoutLabel.toArray).compressed
      (x,y, LabeledPoint(label, featuresMllib))
    }
//
//    val xy = for (x <- 0 until tile.cols;
//                  y <- 0 until tile.rows) yield (x, y)
//
//    val results = xy.map { case (x, y) =>
//      val label = tile.band(classBandNo).getDouble(x, y)
//      val features = for (b <- 0 until tile.bandCount if b != classBandNo) yield tile.band(b).getDouble(x, y)
//      (x, y, label, features)
//    }
//
//    // adjusting the output for MLLIB
//    results.map { case (x, y, label, features) =>
//      val featuresMllib = Vectors.dense(features.toArray).compressed
//      (x, y, LabeledPoint(label, featuresMllib))
//    }
  }

  def MultibandTile2LabelPoint(data: (SpatialKey, MultibandTile)): Array[(LabeledPoint, LabelPointSpatialRef)] = {

    val (spatialKey, tile) = data

    val arrayLP = Array.ofDim[(LabeledPoint, LabelPointSpatialRef)](tile.rows * tile.cols)

    for (y <- 0 until tile.rows; x <- 0 until tile.cols) {
      val bandbuffer = Array.fill(tile.bandCount)(0.0) // Array[Double]
      for (bandno <- 0 until tile.bandCount) {
        bandbuffer(bandno) = tile.band(bandno).get(x, y)
      }

      val bandVector = Vectors.dense(bandbuffer)
      // org.apache.spark.mllib.linalg.Vector
      val classid: Double = 0.0
      // TODO: take class from the vector data
      // ToDo: Apply class id here
      val labeledPixel = LabeledPoint(classid, bandVector.compressed)
      val offset = y * tile.cols + x
      arrayLP(offset) = (labeledPixel, LabelPointSpatialRef(spatialKey, offset))
    }
    arrayLP
  }

  def MultibandTile2LabelPoint(rdd: RDD[(SpatialKey, MultibandTile)]): (RDD[LabeledPoint], RDD[LabelPointSpatialRef]) = {
    // ToDo: Select only pixels within training data
    val data_temp_with_spatialref: RDD[(LabeledPoint, LabelPointSpatialRef)] = rdd
      .flatMap(tile => MultibandTile2LabelPoint(tile))
      .filter(_._1.features.numNonzeros > 0)
    val data_temp: RDD[LabeledPoint] = data_temp_with_spatialref
      .map(item => item._1)
    val spatialref: RDD[LabelPointSpatialRef] = data_temp_with_spatialref
      .map(item => item._2)
    (data_temp, spatialref)
  }

  def SaveAsLibSVMFile(data: (RDD[LabeledPoint], RDD[LabelPointSpatialRef]), trainingName: String): Unit = {
    try {
      val hdfs = org.apache.hadoop.fs.FileSystem.get(data._1.sparkContext.hadoopConfiguration)
      if (hdfs.exists(new org.apache.hadoop.fs.Path(trainingName))) {
        try {
          hdfs.delete(new org.apache.hadoop.fs.Path(trainingName), true)
        } catch {
          case _: Throwable =>
        }
      }
      MLUtils.saveAsLibSVMFile(data._1, trainingName)
    }
    catch {
      case _: Throwable =>
    }
  }

  def SaveAsLibSVMFile(data: RDD[LabeledPoint], trainingName: String): Unit = {
    try {
      val hdfs = org.apache.hadoop.fs.FileSystem.get(data.sparkContext.hadoopConfiguration)
      if (hdfs.exists(new org.apache.hadoop.fs.Path(trainingName))) {
        try {
          hdfs.delete(new org.apache.hadoop.fs.Path(trainingName), true)
        } catch {
          case _: Throwable =>
        }
      }
      MLUtils.saveAsLibSVMFile(data, trainingName)
    }
    catch {
      case _: Throwable =>
    }
  }
}
