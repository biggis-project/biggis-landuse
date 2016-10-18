package biggis.landuse.spark.examples

import com.typesafe.scalalogging.slf4j.LazyLogging
import geotrellis.proj4.WebMercator
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.withTileMethods
import geotrellis.spark.io.file.{FileAttributeStore, FileLayerManager, FileLayerWriter}
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod.spatialKeyIndexMethod
import geotrellis.spark.io.{SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme}
import geotrellis.spark.{LayerId, TileLayerMetadata, TileLayerRDD, withProjectedExtentTilerKeyMethods, withTileRDDReprojectMethods, withTilerMethods}
import org.apache.spark.{SparkConf, SparkContext}


object GeotiffTilingExample extends LazyLogging {

  private val TILE_SIZE = 512
  private val RDD_PARTITIONS = 24
  private val RESAMPLING_METHOD = Bilinear

  /**
    * Run as: /path/to/raster.tif some_layer /path/to/some/dir
    */
  def main(args: Array[String]): Unit = {
    try {
      val Array(inputPath, layerName, catalogPath) = args
      GeotiffTilingExample(inputPath, layerName)(catalogPath)
    } catch {
      case _: MatchError => println("Run as: inputPath layerName /path/to/catalog")
    }
  }

  def apply(inputPath: String, layerName: String)(implicit catalogPath: String) {

    logger info s"Loading geotiff '$inputPath' into '$layerName' in catalog '$catalogPath' ... "

    val sparkConf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Spark Tiler")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    implicit val sc = new SparkContext(sparkConf)

    logger debug "Opening geotiff as RDD"
    val inputRdd = sc.hadoopGeoTiffRDD(inputPath)
    val (_, myRasterMetaData) = TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme(TILE_SIZE))

    val tiled = inputRdd
      .tileToLayout(myRasterMetaData.cellType, myRasterMetaData.layout, RESAMPLING_METHOD)
      .repartition(RDD_PARTITIONS)

    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = TILE_SIZE)

    logger debug "Reprojecting to WebMercator"
    val (zoom, reprojected) =
      TileLayerRDD(tiled, myRasterMetaData).reproject(WebMercator, layoutScheme, RESAMPLING_METHOD)

    // Create the attributes store that will tell us information about our catalog.
    val attributeStore = FileAttributeStore(catalogPath)

    // Create the writer that we will use to store the tiles in the local catalog.
    val writer = FileLayerWriter(attributeStore)
    val layerId = LayerId(layerName, zoom)

    // If the layer exists already, delete it out before writing
    if (attributeStore.layerExists(layerId)) {
      new FileLayerManager(attributeStore).delete(layerId)
    }

    logger debug "Writing reprojected tiles using space filling curve"
    writer.write(layerId, reprojected, ZCurveKeyIndexMethod)

    sc.stop()
    logger debug "Spark context stopped"

    logger info "done."
  }
}