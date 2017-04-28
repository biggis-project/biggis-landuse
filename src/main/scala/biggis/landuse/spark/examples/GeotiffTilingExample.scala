package biggis.landuse.spark.examples

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.WebMercator
import geotrellis.raster.withTileMethods
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerWriter, HadoopSparkContextMethodsWrapper}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod.spatialKeyIndexMethod
import geotrellis.spark.io.{SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme}
import geotrellis.spark.{LayerId, TileLayerMetadata, TileLayerRDD, withProjectedExtentTilerKeyMethods, withTileRDDReprojectMethods, withTilerMethods}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkContext, SparkException}


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
      implicit val sc = Utils.initSparkContext
      GeotiffTilingExample(inputPath, layerName)(catalogPath, sc)
      sc.stop()
    } catch {
      case _: MatchError => println("Run as: inputPath layerName /path/to/catalog")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
    }
  }

  def apply(inputPath: String, layerName: String)(implicit catalogPath: String, sc: SparkContext) {

    logger info s"Loading geotiff '$inputPath' into '$layerName' in catalog '$catalogPath' ... "

    //implicit val sc = Utils.initSparkContext

    logger debug "Opening geotiff as RDD"
    val inputRdd = sc.hadoopGeoTiffRDD(inputPath)
    val (_, myRasterMetaData) = TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme(Utils.TILE_SIZE))

    val tiled = inputRdd
      .tileToLayout(myRasterMetaData.cellType, myRasterMetaData.layout, Utils.RESAMPLING_METHOD)
      .repartition(Utils.RDD_PARTITIONS)

    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = Utils.TILE_SIZE)

    logger debug "Reprojecting to WebMercator"
    val (zoom, reprojected) =
      TileLayerRDD(tiled, myRasterMetaData).reproject(WebMercator, layoutScheme, Utils.RESAMPLING_METHOD)

    // Create the attributes store that will tell us information about our catalog.
    val catalogPathHdfs = new Path(catalogPath)
    val attributeStore = HadoopAttributeStore(catalogPathHdfs)

    // Create the writer that we will use to store the tiles in the local catalog.
    val writer = HadoopLayerWriter(catalogPathHdfs, attributeStore)
    val layerId = LayerId(layerName, zoom)

    // If the layer exists already, delete it out before writing
    if (attributeStore.layerExists(layerId)) {
      logger debug s"Layer $layerId already exists, deleting ..."
      HadoopLayerDeleter(attributeStore).delete(layerId)
    }

    logger debug "Writing reprojected tiles using space filling curve"
    writer.write(layerId, reprojected, ZCurveKeyIndexMethod)

    Utils.writeHistogram(attributeStore, layerName, reprojected.histogram)

    //sc.stop()
    logger info "done."
  }
}
