package biggis.landuse.spark.examples

import com.typesafe.scalalogging.slf4j.StrictLogging
import geotrellis.proj4.WebMercator
import geotrellis.raster.io.HistogramDoubleFormat
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.withTileMethods
import geotrellis.spark.io.file.{FileAttributeStore, FileLayerManager, FileLayerWriter}
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod.spatialKeyIndexMethod
import geotrellis.spark.io.{SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme}
import geotrellis.spark.{LayerId, TileLayerMetadata, TileLayerRDD, withProjectedExtentTilerKeyMethods, withStatsTileRDDMethods, withTileRDDReprojectMethods, withTilerMethods}
import org.apache.spark.{SparkConf, SparkContext, SparkException}

object GeotiffToPyramid extends StrictLogging {

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