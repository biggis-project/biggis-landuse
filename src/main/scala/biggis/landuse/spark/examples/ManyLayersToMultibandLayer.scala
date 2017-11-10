package biggis.landuse.spark.examples

import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.{ArrayMultibandTile, DoubleConstantNoDataCellType, MultibandTile, Tile}
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerReader, HadoopLayerWriter}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod.spatialKeyIndexMethod
import geotrellis.spark.io.{LayerHeader, SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
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
      val (layerNameArray,Array(layerNameOut, catalogPath)) = (args.take(args.size - 2),args.drop(args.size - 2))
      if(args.size == 4){
        val Array(layerName1, layerName2, layerNameOut, catalogPath) = args
        implicit val sc = Utils.initSparkAutoContext  // do not use - only for dirty debugging
        ManyLayersToMultibandLayer(layerName1, layerName2, layerNameOut)(catalogPath, sc)
        sc.stop()
      } else if(args.size > 4){
        //val layerNameArray = args.take(1 + args.size - 2)
        //val Array(layerNameOut, catalogPath) = args.drop(1 + args.size - 2)
        implicit val sc = Utils.initSparkAutoContext  // do not use - only for dirty debugging
        ManyLayersToMultibandLayer( layerNameArray, layerNameOut)(catalogPath, sc)
        sc.stop()
      }
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
    logger info s"using zoom level $commonZoom"

    val layerId1 = findLayerIdByNameAndZoom(layerName1, commonZoom)
    val layerId2 = findLayerIdByNameAndZoom(layerName2, commonZoom)

    println(s"$layerId1, $layerId2")

    val tiles1: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      //layerReader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](layerId1)
      layerReaderMB(layerId1)(layerReader)

    val tiles2: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      //layerReader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](layerId2)
      layerReaderMB(layerId2)(layerReader)

    val outTiles: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      merge2MBlayers(tiles1,tiles2)

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

  def apply(layerNames: Array[String], layerNameOut: String)(implicit catalogPath: String, sc: SparkContext) {
    val layerNamesAll : String = {var layerNamesConcat = "";layerNames.foreach( layerName => layerNamesConcat += "["+layerName+"]");layerNamesConcat}
    logger info s"Combining '$layerNamesAll' into '$layerNameOut' inside '$catalogPath'"

    // Create the attributes store that will tell us information about our catalog.
    val catalogPathHdfs = new Path(catalogPath)
    implicit val attributeStore = HadoopAttributeStore(catalogPathHdfs)
    implicit val layerReader = HadoopLayerReader(attributeStore)

    implicit val commonZoom = getCommonZoom(layerNames)  //Math.max(findFinestZoom(layerName1), findFinestZoom(layerName2)
    logger info s"using zoom level $commonZoom"

    val outTiles: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = mergeMBlayers(layerNames)

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

  def layerReaderMB(layerId: LayerId)(implicit layerReader: HadoopLayerReader) : RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
    try {
      //val schema = layerReader.attributeStore.readSchema(layerId)
      //val meta = layerReader.attributeStore.readMetadata(layerId)
      //val attributes = layerReader.attributeStore.availableAttributes(layerId)
      val header = layerReader.attributeStore.readHeader[LayerHeader](layerId)
      assert(header.keyClass == "geotrellis.spark.SpatialKey")
      //assert(header.valueClass == "geotrellis.raster.Tile")
      if (header.valueClass == "geotrellis.raster.Tile"){
        layerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
          .withContext { rdd =>
            rdd.map { case (spatialKey, tile) => (spatialKey, ArrayMultibandTile(tile)) }
          }
      }
      else {
        assert(header.valueClass == "geotrellis.raster.MultibandTile")
        layerReader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](layerId)
      }
    }
    catch { case _: Throwable => null }
  }

  def getCommonZoom(layerNames: Array[String])(implicit attributeStore: HadoopAttributeStore) : Int = {
    var commonZoom: Int = 0
    layerNames.foreach( layerName => { commonZoom = Math.max(commonZoom, findFinestZoom(layerName))})
    commonZoom
  }
  def getLayerId(layerName: String)(implicit attributeStore: HadoopAttributeStore, commonZoom: Int): LayerId ={
    findLayerIdByNameAndZoom(layerName,commonZoom)
  }

  def merge2MBlayers(tiles1:RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]], tiles2: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]])(implicit tilesize: (Int,Int) = (256,256)): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] ={
    val tilesmerged: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      tiles1.withContext { rdd =>
        rdd.join(tiles2).map { case (spatialKey, (mbtile1, mbtile2)) =>

          val tilebands1 = mbtile1.bands.toArray.map ( band =>
            band.crop(tilesize._1, tilesize._2).convert(DoubleConstantNoDataCellType))
          val tilebands2 = mbtile2.bands.toArray.map ( band =>
            band.crop(tilesize._1, tilesize._2).convert(DoubleConstantNoDataCellType))

          val mbtile = ArrayMultibandTile( tilebands1 ++ tilebands2)

          (spatialKey, mbtile)
        }
      }
    tilesmerged
  }
  def mergeMBlayers(layerNames: Array[String])(implicit commonZoom: Int, attributeStore: HadoopAttributeStore, layerReader: HadoopLayerReader): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
    var tilesmerged : RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = null
    layerNames.foreach( layerName => {
      logger info s"Reading Layer $layerName"
      var tiles : RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = layerReaderMB(getLayerId(layerName))(layerReader)
      if(tilesmerged == null){
        tilesmerged = tiles
      } else {
        tilesmerged = merge2MBlayers(tilesmerged, tiles)
      }
    })
    tilesmerged
  }
}
