package biggis.landuse.spark.examples

import biggis.landuse.api.SpatialMultibandRDD
import com.typesafe.scalalogging.LazyLogging
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.spark.{LayerId, MultibandTileLayerRDD, TileLayerRDD, resample, _}
import org.apache.spark.SparkContext
import org.apache.spark.SparkException

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

    val outputRdd : SpatialMultibandRDD = //inputRdd//.asInstanceOf[MultibandTileLayerRDD[SpatialKey]].resampleToZoom(zoomIn.toInt, zoomOut.toInt)
      if(zoomOut > zoomIn) resampleLayerToZoom(inputRdd, zoomIn.toInt, zoomOut.toInt)   // UpSample (DownLevel) using ZoomResample
      else if(zoomOut < zoomIn) Pyramid.up( inputRdd, ZoomedLayoutScheme(inputRdd.metadata.crs), zoomOut.toInt)._2  // DownSample (UpLevel) using Pyramid.up
      else inputRdd

    biggis.landuse.api.writeRddToLayer(outputRdd, LayerId(layerNameOut, zoomOut.toInt))

    logger info "done."
  }

  implicit def resampleLayerToZoom(rdd: MultibandTileLayerRDD[SpatialKey], zoomLevelIn: Int, zoomLevelOut: Int)(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
    //geotrellis.spark.resample.Implicits.withZoomResampleMultibandMethods(rdd).resampleToZoom(zoomLevelIn, zoomLevelOut)
    geotrellis.spark.resample.Implicits.withLayerRDDZoomResampleMethods(rdd).resampleToZoom(zoomLevelIn, zoomLevelOut)
  }

}