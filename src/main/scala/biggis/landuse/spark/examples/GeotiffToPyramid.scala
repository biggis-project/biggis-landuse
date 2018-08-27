package biggis.landuse.spark.examples

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.WebMercator
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.withTileMethods
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerWriter, HadoopSparkContextMethodsWrapper}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod.spatialKeyIndexMethod
import geotrellis.spark.io.{SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme}
import geotrellis.spark.{LayerId, TileLayerMetadata, TileLayerRDD, withProjectedExtentTilerKeyMethods, withTileRDDReprojectMethods, withTilerMethods}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException

/**
  * This code is now redundant because we can use:
  * GeotiffTilingExample + LayerToPyramid
  */
object GeotiffToPyramid extends LazyLogging {

  def main(args: Array[String]): Unit = {
    try {
      val Array(inputPath, layerName, catalogPath) = args
      GeotiffToPyramid(inputPath, layerName)(catalogPath)
    } catch {
      case _: MatchError => println("Run as: inputPath layerName /path/to/catalog")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
    }
  }

  def apply(inputPath: String, layerName: String)(implicit catalogPath: String) {

    logger debug s"Building the pyramid '$layerName' from geotiff '$inputPath' ... "

    implicit val sc = Utils.initSparkContext

    val inputRdd = sc.hadoopGeoTiffRDD(inputPath)
    val (_, myRasterMetaData) = TileLayerMetadata.fromRDD(inputRdd, FloatingLayoutScheme(Utils.TILE_SIZE))

    val tiled = inputRdd
      .tileToLayout(myRasterMetaData.cellType, myRasterMetaData.layout, Bilinear)
      .repartition(Utils.RDD_PARTITIONS)

    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = Utils.TILE_SIZE)
    val (zoom, reprojected) = TileLayerRDD(tiled, myRasterMetaData)
      .reproject(WebMercator, layoutScheme, Utils.RESAMPLING_METHOD)

    // Create the attributes store that will tell us information about our catalog.
    val catalogPathHdfs = new Path(catalogPath)
    val attributeStore = HadoopAttributeStore( catalogPathHdfs )

    // Create the writer that we will use to store the tiles in the local catalog.
    val writer =  HadoopLayerWriter(catalogPathHdfs, attributeStore)

    // Pyramiding up the zoom levels, write our tiles out to the local file system.
    Pyramid.upLevels(reprojected, layoutScheme, zoom) { (rdd, z) =>
      val layerId = LayerId(layerName, z)

      // If the layer exists already, delete it before writing
      if (attributeStore.layerExists(layerId)) {
        logger debug s"Layer $layerId already exists, deleting ..."
        HadoopLayerDeleter(attributeStore).delete(layerId)
      }

      logger debug s"Writing $layerId tiles using space filling curve"
      writer.write(layerId, rdd, ZCurveKeyIndexMethod)
    }

    Utils.writeHistogram(attributeStore, layerName, reprojected.histogram)

    sc.stop()
    logger debug s"Pyramid '$layerName' is ready in catalog '$catalogPath'"
  }

}