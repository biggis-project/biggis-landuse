package biggis.landuse.spark.examples

import com.typesafe.scalalogging.slf4j.LazyLogging
import geotrellis.proj4.WebMercator
import geotrellis.raster.io.HistogramDoubleFormat
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.withTileMethods
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerWriter, HadoopSparkContextMethodsWrapper}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod.spatialKeyIndexMethod
import geotrellis.spark.io.{SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme}
import geotrellis.spark.{LayerId, TileLayerMetadata, TileLayerRDD, withProjectedExtentTilerKeyMethods, withTileRDDReprojectMethods, withTilerMethods}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext, SparkException}


/**
  * Within this example:
  * - Geotiff raster file is opened using Spark RDD
  * - the raster is reprojected to WebMercator
  * - the raster is tiled into a grid
  * - all tiles are stored as a layer in geotrellis catalog
  */
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
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
    }
  }

  def apply(inputPath: String, layerName: String)(implicit catalogPath: String) {

    logger info s"Loading geotiff '$inputPath' into '$layerName' in catalog '$catalogPath' ... "

    val sparkConf =
      new SparkConf()
        .setAppName("Spark Tiler")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    // We also need to set the spark master.
    // instead of  hardcoding it using spakrConf.setMaster("local[*]")
    // we can use the JVM parameter: -Dspark.master=local[*]
    // sparkConf.setMaster("local[*]")

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
    val catalogPathHdfs = new Path(catalogPath)
    val attributeStore = HadoopAttributeStore( catalogPathHdfs )

    // Create the writer that we will use to store the tiles in the local catalog.
    val writer =  HadoopLayerWriter(catalogPathHdfs, attributeStore)
    val layerId = LayerId(layerName, zoom)

    // If the layer exists already, delete it out before writing
    if (attributeStore.layerExists(layerId)) {
      logger debug s"Layer $layerId already exists, deleting ..."
      HadoopLayerDeleter(attributeStore).delete(layerId)
    }

    logger debug "Writing reprojected tiles using space filling curve"
    writer.write(layerId, reprojected, ZCurveKeyIndexMethod)

    logger debug "Writing attribute 'histogramData' for zoom=0"
    writer.attributeStore.write(
      LayerId(layerName, 0), "histogramData", reprojected.histogram)

    sc.stop()
    logger debug "Spark context stopped"
    logger info "done."
  }
}
