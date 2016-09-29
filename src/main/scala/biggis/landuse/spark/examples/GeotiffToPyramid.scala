package biggis.landuse.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.typesafe.scalalogging.slf4j.StrictLogging

import geotrellis.proj4.WebMercator
import geotrellis.raster.io.HistogramDoubleFormat
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.withTileMethods
import geotrellis.spark.LayerId
import geotrellis.spark.TileLayerMetadata
import geotrellis.spark.TileLayerRDD
import geotrellis.spark.io.SpatialKeyFormat
import geotrellis.spark.io.file.FileAttributeStore
import geotrellis.spark.io.file.FileLayerManager
import geotrellis.spark.io.file.FileLayerWriter
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod.spatialKeyIndexMethod
import geotrellis.spark.io.spatialKeyAvroFormat
import geotrellis.spark.io.tileLayerMetadataFormat
import geotrellis.spark.io.tileUnionCodec
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.spark.withProjectedExtentTilerKeyMethods
import geotrellis.spark.withStatsTileRDDMethods
import geotrellis.spark.withTileRDDReprojectMethods
import geotrellis.spark.withTilerMethods

object GeotiffToPyramid extends StrictLogging {

  /**
    * Run as: /path/to/raster.tif some_layer /path/to/some/dir
    */
  def main(args: Array[String]): Unit = {
    val Array(inputPath, layerName, catalogPath) = args
    GeotiffToPyramid(inputPath, layerName)(catalogPath)
  }

  def apply(inputPath: String, layerName: String)(implicit catalogPath: String) {

    logger debug s"Building the pyramid '$layerName' from geotiff '$inputPath' ... "
    logger info "hi"
    val sparkConf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Spark Tiler")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    implicit val sc = new SparkContext(sparkConf)

    val inputRdd = sc.hadoopGeoTiffRDD(inputPath)
    val (_, myRasterMetaData) = TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme(512))

    val tiled = inputRdd
      .tileToLayout(myRasterMetaData.cellType, myRasterMetaData.layout, Bilinear)
      .repartition(12)

    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 512)
    val (zoom, reprojected) = TileLayerRDD(tiled, myRasterMetaData).reproject(WebMercator, layoutScheme, Bilinear)

    // Create the attributes store that will tell us information about our catalog.
    val attributeStore = FileAttributeStore(catalogPath)

    // Create the writer that we will use to store the tiles in the local catalog.
    val writer = FileLayerWriter(attributeStore)

    val hist = reprojected.histogram()

    // Pyramiding up the zoom levels, write our tiles out to the local file system.
    Pyramid.upLevels(reprojected, layoutScheme, zoom) { (rdd, z) =>
      val layerId = LayerId(layerName, z)

      // If the layer exists already, delete it out before writing
      if (attributeStore.layerExists(layerId)) {
        new FileLayerManager(attributeStore).delete(layerId)
      }

      writer.write(layerId, rdd, ZCurveKeyIndexMethod)

      if (z == 0) {
        val id = LayerId(layerName, 0)
        writer.attributeStore.write(id, "histogramData", hist)
      }
    }

    sc.stop()

    logger debug s"Pyramid '$layerName' is ready in catalog '$catalogPath'"
    logger debug s"Quantile breaks from histogram: ${hist.quantileBreaks(10).mkString(", ")}"
  }

}