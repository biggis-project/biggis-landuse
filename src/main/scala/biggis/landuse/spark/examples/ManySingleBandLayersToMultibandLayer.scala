package biggis.landuse.spark.examples

import biggis.landuse.spark.examples.MultibandGeotiffTilingExample.logger
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.{ArrayMultibandTile, DoubleConstantNoDataCellType, IntConstantNoDataCellType, MultibandTile, Tile}
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerReader, HadoopLayerWriter}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import geotrellis.spark.io.{SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod.spatialKeyIndexMethod

/**
  * Created by vlx on 1/19/17.
  */
object ManySingleBandLayersToMultibandLayer extends App with LazyLogging {

  val layerName1 = "morning2"
  val layerName2 = "morning2_conv"
  val layerNameOut = "mblayer"
  val catalogPath = "target/geotrellis-catalog"

  logger info s"Combining '$layerName1' and '$layerName2' into $layerNameOut inside '$catalogPath'"

  implicit val sc = Utils.initLocalSparkContext

  // Create the attributes store that will tell us information about our catalog.
  val catalogPathHdfs = new Path(catalogPath)
  val attributeStore = HadoopAttributeStore( catalogPathHdfs )
  val layerReader = HadoopLayerReader(attributeStore)

  val commonZoom = Math.max(findFinestZoom(layerName1), findFinestZoom(layerName2))

  val layerId1 = findLayerIdByNameAndZoom(layerName1, commonZoom)
  val layerId2 = findLayerIdByNameAndZoom(layerName2, commonZoom)

  println(s"$layerId1, $layerId2")

  val tiles1:RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
    layerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId1)

  val tiles2:RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
    layerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId2)

  val outTiles:RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] =
    tiles1.withContext { rdd =>
      rdd.join(tiles2).map { case (spatialKey, (tile1, tile2)) =>
        val mbtile = ArrayMultibandTile(
          tile2.crop(256, 256).convert(DoubleConstantNoDataCellType),
          tile1.crop(256, 256).convert(DoubleConstantNoDataCellType)
        ).convert(DoubleConstantNoDataCellType)

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

  sc.stop()
  logger info "done."

  def findFinestZoom(layerName:String):Int = {
    val zoomsOfLayer = attributeStore.layerIds filter (_.name == layerName)
    zoomsOfLayer.sortBy(_.zoom).last.zoom
  }

  def findLayerIdByNameAndZoom(layerName:String, zoom:Int):LayerId = {
    val zoomsOfLayer = attributeStore.layerIds filter (_.name == layerName)
    zoomsOfLayer.filter(_.zoom == zoom).head
  }
}
