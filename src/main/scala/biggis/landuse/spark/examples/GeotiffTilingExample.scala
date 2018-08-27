package biggis.landuse.spark.examples

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.WebMercator
import geotrellis.raster.withTileMethods
import geotrellis.spark.LayerId
import geotrellis.spark.TileLayerMetadata
import geotrellis.spark.TileLayerRDD
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.spark.withProjectedExtentTilerKeyMethods
import geotrellis.spark.withTileRDDReprojectMethods
import geotrellis.spark.withTilerMethods
import org.apache.spark.SparkContext
import org.apache.spark.SparkException


/**
  * Within this example:
  * - Geotiff raster file is opened as a Spark RDD
  * - the raster is reprojected to WebMercator
  * - the raster is tiled into a grid
  * - all tiles are stored as a layer in geotrellis catalog
  * - histogram data are stored as an attribute in the catalog (into zoom level 0)
  */
object GeotiffTilingExample extends LazyLogging {

  /**
    * Run as: /path/to/raster.tif some_layer /path/to/some/dir
    */
  def main(args: Array[String]): Unit = {
    try {
      val Array(inputPath, layerName, catalogPath) = args
      implicit val sc = Utils.initSparkAutoContext
      GeotiffTilingExample(inputPath, layerName)(catalogPath, sc)
      sc.stop()
    } catch {
      case _: MatchError => println("Run as: inputPath layerName /path/to/catalog")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
    }
  }

  def apply(inputPath: String, layerName: String)(implicit catalogPath: String, sc: SparkContext) {

    logger info s"Loading geotiff '$inputPath' into '$layerName' in catalog '$catalogPath' ... "

    logger debug "Opening geotiff as RDD"
    val inputRdd = sc.hadoopGeoTiffRDD(inputPath)
    val (_, myRasterMetaData) = TileLayerMetadata.fromRDD(inputRdd, FloatingLayoutScheme(Utils.TILE_SIZE))

    val tiled = inputRdd
      .tileToLayout(myRasterMetaData.cellType, myRasterMetaData.layout, Utils.RESAMPLING_METHOD)
      .repartition(Utils.RDD_PARTITIONS)

    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = Utils.TILE_SIZE)

    logger debug "Reprojecting to WebMercator"
    val (zoom, reprojected) =
      TileLayerRDD(tiled, myRasterMetaData).reproject(WebMercator, layoutScheme, Utils.RESAMPLING_METHOD)

    biggis.landuse.api.writeRddToLayer(reprojected, LayerId(layerName, zoom))

    //sc.stop()
    logger info "done."
  }
}
