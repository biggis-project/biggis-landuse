package biggis.landuse.spark.examples

import com.typesafe.scalalogging.slf4j.LazyLogging
import geotrellis.raster.io.HistogramDoubleFormat
import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster.{Tile, withTileMethods}
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerReader, HadoopLayerWriter}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod.spatialKeyIndexMethod
import geotrellis.spark.io.{SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD

object TilePixelingExample extends LazyLogging {

  def main(args: Array[String]): Unit = {
    try {
      val Array(layerName, catalogPath) = args
      TilePixelingExample(layerName)(catalogPath)
    } catch {
      case _: MatchError => println("Run as: layerName /path/to/catalog")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
    }
  }

  def apply(layerName: String)(implicit catalogPath: String): Unit = {
    logger info s"Running convolution of layer '$layerName' in catalog '$catalogPath'"

    implicit val sc = Utils.initSparkContext

    // Create the attributes store that will tell us information about our catalog.
    val catalogPathHdfs = new Path(catalogPath)
    val attributeStore = HadoopAttributeStore( catalogPathHdfs )
    val layerReader = HadoopLayerReader(attributeStore)

    val zoomsOfLayer = attributeStore.layerIds filter (_.name == layerName)
    if (zoomsOfLayer.isEmpty) {
      logger info s"Layer '$layerName' not found in the catalog '$catalogPath'"
      return
    }

    val srcLayerId = zoomsOfLayer.sortBy(_.zoom).last
    logger debug s"The following layerId will be used: $srcLayerId"

    val queryResult: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = layerReader
      .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](srcLayerId)

    // TODO: adjust this code to produce stream of pixels from each tile (flatmap)
//    val convolvedLayerRdd = queryResult
//        .flatMapValues { v =>
//          // TODO: convert tile to pixels here
//        }
//    }

    // TODO: write the stram of pixels into a huge file

    sc.stop()
    logger info "done."
  }
}
