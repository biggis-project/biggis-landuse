package biggis.landuse.spark.examples

import geotrellis.raster.MultibandTile
import geotrellis.raster.Tile
import geotrellis.spark.{SpaceTimeKey, SpatialKey, TemporalKey}
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

import scala.util.matching.Regex

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

  case class pathContents (dir: String, filename : String, suffix: String, dirbase: String, filebase: String, filetype: String, dir_hierarchy: Iterable[String])
  def ParsePath(path: String): pathContents = {
    val regexp = "((.*)[\\\\/])?(([^\\\\/]*?)(\\.([^.]*))?)$".r
    val regexp(dir, dirbase, filename, filebase, suffix, filetype) = path
    val hierarchy = dirbase.split("\\/").toIterable
    pathContents(dir, filename, suffix, dirbase, filebase, filetype, hierarchy)
  }
  def DeleteFile(fileName: String)(implicit sc : SparkContext): Unit = {
    try {
      val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
      if (hdfs.exists(new org.apache.hadoop.fs.Path(fileName))) {
        try {
          hdfs.delete(new org.apache.hadoop.fs.Path(fileName), true)
        } catch {
          case _: Throwable =>
        }
      }
    }
    catch {
      case _: Throwable =>
    }
  }
  def SaveAsLibSVMFile(data: RDD[LabeledPoint], trainingName: String)(implicit removeZeroLabel: Boolean = false): Unit = {
    try {
      implicit val sc = data.sparkContext
      val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
      val trainingPath = ParsePath(trainingName)
      val first_dir = trainingPath.dir_hierarchy.toArray.apply(1)
      val use_single_file_export = trainingPath.filetype=="txt"
      if(use_single_file_export){
        val trainingNameTemp = trainingName+"_temp"
        DeleteFile(trainingNameTemp)
        if(removeZeroLabel){
          val data_w_o_nodata = data.filter( _.label > 0)
          MLUtils.saveAsLibSVMFile(data_w_o_nodata, trainingNameTemp)
        } else
          MLUtils.saveAsLibSVMFile(data, trainingNameTemp)
        DeleteFile(trainingName)
        FileUtil.copyMerge(hdfs, new Path(trainingNameTemp), hdfs, new Path(trainingName), true, sc.hadoopConfiguration, null)
        DeleteFile(trainingNameTemp)
      }
      else {
        DeleteFile(trainingName)
        if(removeZeroLabel){
          val data_w_o_nodata = data.filter( _.label > 0)
          MLUtils.saveAsLibSVMFile(data_w_o_nodata, trainingName)
        } else
          MLUtils.saveAsLibSVMFile(data, trainingName)
      }
   }
    catch {
      case _: Throwable =>
    }
  }

}
