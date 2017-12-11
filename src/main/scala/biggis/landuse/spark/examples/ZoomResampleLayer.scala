package biggis.landuse.spark.examples

import biggis.landuse.api.SpatialMultibandRDD
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.spark.{LayerId, MultibandTileLayerRDD, TileLayerRDD, resample, _}
import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import geotrellis.spark.resample._
import geotrellis.spark.resample.Implicits
import geotrellis.spark.resample.ZoomResample
import geotrellis.spark.resample.ZoomResampleMethods
import geotrellis.raster._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.vector.Extent
import org.scalatest.FunSpec


object ZoomResampleLayer extends LazyLogging {
  /**
    * Run as: layerNameIn zoomIn layerNameOut zoomOut /path/to/catalog
    */
  def main(args: Array[String]): Unit = {
    try {
      val Array(layerNameIn, zoomIn, layerNameOut, zoomOut, catalogPath) = args
      implicit val sc : SparkContext = Utils.initSparkAutoContext
      ZoomResampleLayer(layerNameIn, zoomIn, layerNameOut, zoomOut)(catalogPath, sc)
      sc.stop()
    } catch {
      case _: MatchError => println("Run as: layerNameIn zoomIn layerNameOut zoomOut /path/to/catalog")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parameter: -Dspark.master=local[*]"
    }
  }

  def apply(layerNameIn: String, zoomIn: String, layerNameOut: String, zoomOut: String)(implicit catalogPath: String, sc: SparkContext) {

    logger info s"Resampling layer '$layerNameIn' with '$zoomOut' into '$layerNameOut' with '$zoomOut' in catalog '$catalogPath' ... "

    val inputRdd : SpatialMultibandRDD = biggis.landuse.api.readRddFromLayer((layerNameIn, zoomIn.toInt))

    //val countTilesIn = inputRdd.count()

    //val layer : TileLayerRDD[SpatialKey] = ContextRDD(inputRdd.distinct().asInstanceOf[TileLayerRDD[SpatialKey]], inputRdd.metadata)

    //layer.resampleToZoom(zoomIn.toInt, zoomOut.toInt) // exists for Tile, but not for MultibandTile

    val outputRdd : SpatialMultibandRDD = //inputRdd//.asInstanceOf[MultibandTileLayerRDD[SpatialKey]].resampleToZoom(zoomIn.toInt, zoomOut.toInt)
      resampleLayerToZoom(inputRdd, zoomIn.toInt, zoomOut.toInt)

    biggis.landuse.api.writeRddToLayer(outputRdd, LayerId(layerNameOut, zoomOut.toInt))

    logger info "done."
  }

  implicit def resampleLayerToZoom(rdd: MultibandTileLayerRDD[SpatialKey], zoomLevelIn: Int, zoomLevelOut: Int)(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
    geotrellis.spark.resample.Implicits.withZoomResampleMultibandMethods(rdd).resampleToZoom(zoomLevelIn, zoomLevelOut)
  }

}