package biggis.landuse.spark.examples

import com.typesafe.scalalogging.slf4j.LazyLogging
import geotrellis.raster.{Tile, withTileMethods}
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io.file.{FileAttributeStore, FileLayerManager, FileLayerReader, FileLayerWriter}
import geotrellis.spark.io.{SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import geotrellis.raster.{io => _, _}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, _}
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerReader}
import geotrellis.spark.stitch._
import geotrellis.spark.{io => _, _}
import org.apache.hadoop.fs.Path

// https://github.com/geotrellis/geotrellis/blob/master/docs/spark/spark-examples.md

object MultibandLayerToGeotiff extends LazyLogging{
  def main(args: Array[String]): Unit = {
    try {
      val Array(layerName, outputPath, catalogPath) = args
      MultibandLayerToGeotiff(layerName, outputPath)(catalogPath)
    } catch {
      case _: MatchError => println("Run as: layerName outputPath /path/to/catalog")
    }
  }

  def apply(layerName: String, outputPath: String)(implicit catalogPath: String): Unit = {
    logger info s"Writing layer '$layerName' in catalog '$catalogPath' to '$outputPath'"

    implicit val sc = Utils.initSparkContext

    val catalogPathHdfs = new Path(catalogPath)
    val attributeStore = HadoopAttributeStore(catalogPathHdfs)
    val layerReader = HadoopLayerReader(attributeStore)

    val zoomsOfLayer = attributeStore.layerIds filter (_.name == layerName)
    if(zoomsOfLayer.isEmpty) {
      logger info s"Layer '$layerName' not found in the catalog '$catalogPath'"
      return
    }

    val srcLayerId = zoomsOfLayer.sortBy(_.zoom).last
    logger debug s"The following layerId will be used: $srcLayerId"

    val inputRdd:RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = layerReader
      .read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](srcLayerId)

    val metadata = inputRdd.metadata

    val tiled: RDD[(SpatialKey, MultibandTile)] = inputRdd.distinct()

    val tile: MultibandTile = tiled.stitch()

    val crs = metadata.crs
    //val raster: Raster[MultibandTile] = tile.reproject(metadata.extent, metadata.crs, metadata.crs)
    MultibandGeoTiff(tile, metadata.extent, crs).write(outputPath)

    sc.stop()
    logger debug "Spark context stopped"

    logger info "done."
  }
}
