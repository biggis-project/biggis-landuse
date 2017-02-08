package biggis.landuse.spark.examples

import geotrellis.raster.MultibandTile
import geotrellis.raster.Tile
import geotrellis.spark.{Metadata, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification.SVMMultiClassOVAModel

/**
  * Renamed by ak on 26.01.2017.
  */
@deprecated("do not use, except for debugging, replace by UtilsML")
object UtilsSVM extends biggis.landuse.spark.examples.UtilsML {
  case class BandNoLabel(classBandNo: Int)
  def MultibandTile2xyLabeledPoint( data : RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] )(implicit classBandNo : BandNoLabel = BandNoLabel(-1) ): RDD[(SpatialKey, (Int, Int, LabeledPoint))] = {
    val samples: RDD[(SpatialKey, (Int, Int, LabeledPoint))] with Metadata[TileLayerMetadata[SpatialKey]] =
      data.withContext { rdd =>
        rdd.flatMapValues(mbtile =>
          UtilsML.MultibandTile2LabeledPixelSamples(mbtile, classBandNo = classBandNo.classBandNo )
        )
      }
    samples
  }
  def MultibandTile2LabeledPoint( data : RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] )(implicit classBandNo : BandNoLabel = BandNoLabel(-1) ): RDD[LabeledPoint] = {
    val samples = MultibandTile2xyLabeledPoint(data)
    val lp = samples
      .map( sample => sample._2._3 )
      .filter(_.features.numNonzeros > 0)
    lp
  }

  def SplitSamples( samples : RDD[LabeledPoint], factor : Double): (RDD[LabeledPoint], RDD[LabeledPoint]) = {
    // Split data into training (60%) and test (40%).
    val splits = samples.randomSplit(Array(factor, 1.0-factor), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    (training,test)
  }

  def SaveSVMClassifier( model_multi : SVMMultiClassOVAModel, svmClassifier : String)(implicit sc : SparkContext): Unit ={
    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
    if (hdfs.exists(new org.apache.hadoop.fs.Path(svmClassifier))) {
      try {
        hdfs.delete(new org.apache.hadoop.fs.Path(svmClassifier), true)
      } catch {
        case _: Throwable =>
      }
    }
    model_multi.save(sc, svmClassifier)
  }

  @deprecated("do not use, replace by UtilsML.MultibandTile2LabeledPixelSamples")
  case class LabelPointSpatialRef(spatialKey: SpatialKey, offset: Int)

  @deprecated("do not use, replace by UtilsML.MultibandTile2LabeledPixelSamples")
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

  @deprecated("do not use, replace by UtilsML.MultibandTile2LabeledPixelSamples")
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

  @deprecated("do not use, replace by UtilsML.SaveAsLibSVMFile")
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

}
