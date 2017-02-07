package biggis.landuse.spark.examples

import com.typesafe.scalalogging.slf4j.LazyLogging
import geotrellis.raster.{ArrayMultibandTile, DoubleConstantNoDataCellType, MultibandTile, Tile}
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerReader, HadoopLayerWriter}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod.spatialKeyIndexMethod
import geotrellis.spark.io.{SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.rdd.RDD

/**
  * Created by vlx on 1/19/17.
  */
object ManyLayersToMultibandLayer extends LazyLogging {  //extends App with LazyLogging
  def main(args: Array[String]): Unit = {
    try {
      val Array(layerName1, layerName2, layerNameOut, catalogPath) = args
      implicit val sc = Utils.initSparkContext  // do not use - only for dirty debugging
      ManyLayersToMultibandLayer(layerName1, layerName2, layerNameOut)(catalogPath, sc)
      sc.stop()
      logger debug "Spark context stopped"
    } catch {
      case _: MatchError => println("Run as: inputPath layerName /path/to/catalog")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
    }
  }

  def apply(layerName1: String, layerName2: String, layerNameOut: String)(implicit catalogPath: String, sc: SparkContext) {
    //val layerName1 = "morning2"
    //val layerName2 = "morning2_conv"
    //val layerNameOut = "mblayer"
    //val catalogPath = "target/geotrellis-catalog"

    logger info s"Combining '$layerName1' and '$layerName2' into $layerNameOut inside '$catalogPath'"

    //implicit val sc = Utils.initLocalSparkContext

    // Create the attributes store that will tell us information about our catalog.
    val catalogPathHdfs = new Path(catalogPath)
    implicit val attributeStore = HadoopAttributeStore(catalogPathHdfs)
    val layerReader = HadoopLayerReader(attributeStore)

    val commonZoom = Math.max(findFinestZoom(layerName1), findFinestZoom(layerName2))

    val layerId1 = findLayerIdByNameAndZoom(layerName1, commonZoom)
    val layerId2 = findLayerIdByNameAndZoom(layerName2, commonZoom)

    println(s"$layerId1, $layerId2")

    val tiles1: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      layerReader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](layerId1)

    val tiles2: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      layerReader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](layerId2)

    val outTiles: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      tiles1.withContext { rdd =>
        rdd.join(tiles2).map { case (spatialKey, (mbtile1, mbtile2)) =>

          val tilebands1 = mbtile1.bands.toArray.map ( band =>
            band.crop(256, 256).convert(DoubleConstantNoDataCellType))
          val tilebands2 = mbtile2.bands.toArray.map ( band =>
            band.crop(256, 256).convert(DoubleConstantNoDataCellType))

          val mbtile = ArrayMultibandTile( tilebands1 ++ tilebands2)

          (spatialKey, mbtile)
        }
      }

    // Create the writer that we will use to store the tiles in the local catalog.
    val writer = HadoopLayerWriter(catalogPathHdfs, attributeStore)
    val layerIdOut = LayerId(layerNameOut, commonZoom)

    // If the layer exists already, delete it out before writing
    if (attributeStore.layerExists(layerIdOut)) {
      logger debug s"Layer $layerIdOut already exists, deleting ..."
      HadoopLayerDeleter(attributeStore).delete(layerIdOut)
    }

    logger debug "Writing reprojected tiles using space filling curve"
    writer.write(layerIdOut, outTiles, ZCurveKeyIndexMethod)

    //sc.stop()
    logger info "done."

  }

  def findFinestZoom(layerName: String)(implicit attributeStore: HadoopAttributeStore): Int = {
    val zoomsOfLayer = attributeStore.layerIds filter (_.name == layerName)
    zoomsOfLayer.sortBy(_.zoom).last.zoom
  }

  def findLayerIdByNameAndZoom(layerName: String, zoom: Int)(implicit attributeStore: HadoopAttributeStore): LayerId = {
    val zoomsOfLayer = attributeStore.layerIds filter (_.name == layerName)
    zoomsOfLayer.filter(_.zoom == zoom).head
  }
}
