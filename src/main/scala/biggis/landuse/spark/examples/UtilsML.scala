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
object UtilsML extends UtilsML
trait UtilsML{
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
      val label = if(classBandNo >= 0 ) features(classBandNo) else Double.NaN
      val featuresWithoutLabel = features.take(classBandNo) ::: features.drop(classBandNo + 1)
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
