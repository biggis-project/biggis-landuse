package biggis.landuse.spark.examples

import com.typesafe.scalalogging.slf4j.LazyLogging
import geotrellis.proj4.WebMercator
import geotrellis.raster.Tile
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerReader, HadoopLayerWriter}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.{SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme}
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.rdd.RDD

object LayerToPyramid extends LazyLogging {

  def main(args: Array[String]): Unit = {
    try {
      val Array(catalogPath, layerName) = args
      implicit val sc = Utils.initSparkContext
      LayerToPyramid(catalogPath, layerName)
      sc.stop()
    } catch {
      case _: MatchError => println("Run as: /path/to/catalog layerName")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
    }
  }

  def apply(catalogPath: String, layerName: String)(implicit sc: SparkContext): Unit = {
    logger debug s"Building the pyramid from '$layerName' in catalog $catalogPath ..."

    //implicit val sc = Utils.initSparkContext

    // Create the attributes store that will tell us information about our catalog.
    val catalogPathHdfs = new Path(catalogPath)
    val attributeStore = HadoopAttributeStore(catalogPathHdfs)
    val layerReader = HadoopLayerReader(attributeStore)

    val zoomsOfLayer = attributeStore.layerIds filter (_.name == layerName)
    if (zoomsOfLayer.isEmpty) {
      logger info s"Layer '$layerName' not found in the catalog '$catalogPath'"
      return
    }

    val srcLayerId = zoomsOfLayer.sortBy(_.zoom).last
    logger debug s"The following layerId will be used: $srcLayerId"

    val inputRdd: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = layerReader
      .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](srcLayerId)

    // TODO: figure out how to extract the layoutScheme from the inputRdd
    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = Utils.TILE_SIZE)

    // Create the writer that we will use to store the tiles in the local catalog.
    val writer = HadoopLayerWriter(catalogPathHdfs, attributeStore)

    // Pyramiding up the zoom levels, write our tiles out to the local file system.
    Pyramid.upLevels(inputRdd, layoutScheme, srcLayerId.zoom) { (rdd, z) =>
      val layerId = LayerId(layerName, z)
      if (!attributeStore.layerExists(layerId)) {
        logger info s"Writing $layerId tiles using space filling curve"
        writer.write(layerId, rdd, ZCurveKeyIndexMethod)
      }
    }

    // writing the histogram is only needed if we create a new layer
    // Utils.writeHistogram(attributeStore, layerName + "_p", inputRdd.histogram)

    //sc.stop()
    logger debug "Spark context stopped"
    logger debug s"Pyramid '$layerName' is ready in catalog '$catalogPath'"
  }

}