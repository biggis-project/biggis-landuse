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
import biggis.landuse.api
import biggis.landuse.api.SpatialMultibandRDD
import org.apache.spark.sql.catalog.Catalog

/**
  * Created by vlx on 1/19/17.
  */
object ManyLayersToMultibandLayer extends LazyLogging {  //extends App with LazyLogging
  def main(args: Array[String]): Unit = {
    try {
      val (layerNameArray,Array(layerStackNameOut, catalogPath)) = (args.take(args.length - 2),args.drop(args.length - 2))
      if(args.length == 4){
        val Array(layerName1, layerName2, layerStackNameOut, catalogPath) = args
        implicit val sc : SparkContext = Utils.initSparkAutoContext  // only for debugging - needs rework
        ManyLayersToMultibandLayer(layerName1, layerName2, layerStackNameOut)(catalogPath, sc)
        sc.stop()
      } else if(args.length > 4){
        //val layerNameArray = args.take(1 + args.size - 2)
        //val Array(layerNameOut, catalogPath) = args.drop(1 + args.size - 2)
        implicit val sc : SparkContext = Utils.initSparkAutoContext  // only for debugging - needs rework
        ManyLayersToMultibandLayer( layerNameArray, layerStackNameOut)(catalogPath, sc)
        sc.stop()
      }
      logger debug "Spark context stopped"
    } catch {
      case _: MatchError => println("Run as: inputLayerName1 inputLayerName2 [inputLayerName3 ...] layerStackNameOut /path/to/catalog")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parameter: -Dspark.master=local[*]"
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
    implicit val attributeStore : HadoopAttributeStore = HadoopAttributeStore(catalogPathHdfs)
    val layerReader = HadoopLayerReader(attributeStore)

    //val commonZoom = Math.max(findFinestZoom(layerName1), findFinestZoom(layerName2))
    val commonZoom = findFinestZoom(List(layerName1,layerName2))
    logger info s"using zoom level $commonZoom"

    val layerId1 = findLayerIdByNameAndZoom(layerName1, commonZoom)
    val layerId2 = findLayerIdByNameAndZoom(layerName2, commonZoom)

    println(s"$layerId1, $layerId2")

    /*
    val tiles1: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      //layerReader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](layerId1)
      layerReaderMB(layerId1)(layerReader)

    val tiles2: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      //layerReader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](layerId2)
      layerReaderMB(layerId2)(layerReader)
    */
    val tiles1 : SpatialMultibandRDD = biggis.landuse.api.readRddFromLayer(layerId1)
    val tiles2 : SpatialMultibandRDD = biggis.landuse.api.readRddFromLayer(layerId2)

    val outTiles: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      stack2MBlayers(tiles1,tiles2)

    /*
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
    */

    biggis.landuse.api.deleteLayerFromCatalog(layerNameOut, commonZoom)
    biggis.landuse.api.writeRddToLayer( outTiles, (layerNameOut, commonZoom))

    //sc.stop()
    logger info "done."

  }

  def apply(layerNames: Array[String], layerNameOut: String)(implicit catalogPath: String, sc: SparkContext) {
    val layerNamesAll : String = {var layerNamesConcat = "";layerNames.foreach( layerName => layerNamesConcat += "["+layerName+"]");layerNamesConcat}
    logger info s"Combining '$layerNamesAll' into '$layerNameOut' inside '$catalogPath'"

    /*
    // Create the attributes store that will tell us information about our catalog.
    val catalogPathHdfs = new Path(catalogPath)
    implicit val attributeStore = HadoopAttributeStore(catalogPathHdfs)
    */
    implicit val attributeStore : HadoopAttributeStore = biggis.landuse.api.catalogToStore(catalogPath)
    implicit val layerReader : HadoopLayerReader = HadoopLayerReader(attributeStore)

    implicit val commonZoom : Int = findFinestZoom(layerNames)  //Math.max(findFinestZoom(layerName1), findFinestZoom(layerName2)
    logger info s"using zoom level $commonZoom"

    val outTiles: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = createLayerStack(layerNames)

    /*
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
    */

    biggis.landuse.api.deleteLayerFromCatalog(layerNameOut, commonZoom)
    biggis.landuse.api.writeRddToLayer( outTiles, (layerNameOut, commonZoom))

    //sc.stop()
    logger info "done."

  }

  def findFinestZoom(layerName: String)(implicit attributeStore: HadoopAttributeStore): Int = {
    val zoomsOfLayer = attributeStore.layerIds filter (_.name == layerName)
    if (zoomsOfLayer.isEmpty){
      logger info s"Layer not found: $layerName"
      throw new RuntimeException(s"Layer not found : $layerName")
    }
    zoomsOfLayer.maxBy(_.zoom).zoom  //.sortBy(_.zoom).last.zoom
  }

  def findLayerIdByNameAndZoom(layerName: String, zoom: Int)(implicit attributeStore: HadoopAttributeStore): LayerId = {
    val zoomsOfLayer = attributeStore.layerIds filter (_.name == layerName)
    zoomsOfLayer.filter(_.zoom == zoom).head
  }

  /*
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
  */

  def findFinestZoom(layerNames: Iterable[String])(implicit attributeStore: HadoopAttributeStore) : Int = {
    var commonZoom: Int = 0
    layerNames.foreach( layerName => { commonZoom = Math.max(commonZoom, findFinestZoom(layerName))})
    commonZoom
  }

  def getLayerId(layerName: String)(implicit attributeStore: HadoopAttributeStore, commonZoom: Int): LayerId ={
    findLayerIdByNameAndZoom(layerName,commonZoom)
  }

  def stack2MBlayers(tiles1:RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]], tiles2: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]])(implicit tilesize: (Int,Int) = (256,256)): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] ={
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

  /* *
    * Read leayer from RDD
    * @param layerId     layerName and zoom level
    * @param bandNumber  Optional: select specific band number from layer (only applies to reading MultibandTile as Tile, ignored otherwise), defaults to 0 (first band) if None
    * @param catalogPath Geotrellis catalog
    * @param sc          SparkContext
    * @return            RDD[(K, V)] with Metadata[M] representing a layer of tiles
    * /
  def readRddFromLayerT[T]
  (layerId: LayerId, bandNumber : Option[Int] = None : Option[Int])
  (implicit catalogPath: String, sc: SparkContext, ttag : TypeTag[T]): T = {
    if(ttag.tpe =:= typeOf[SpatialRDD]) readRddFromLayer[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId).asInstanceOf[T]
    else if(ttag.tpe =:= typeOf[SpatialMultibandRDD]) readRddFromLayer[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](layerId).asInstanceOf[T]
    else if(ttag.tpe =:= typeOf[SpaceTimeMultibandRDD]) readRddFromLayer[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](layerId).asInstanceOf[T]
    else if(ttag.tpe =:= typeOf[SpaceTimeRDD]) readRddFromLayer[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](layerId).asInstanceOf[T]
    else {
      throw new RuntimeException("we did not expect any other type than SpatialRDD, SpaceTimeRDD, SpatialMultibandRDD, SpaceTimeMultibandRDD")
      sc.emptyRDD[(T, T)].asInstanceOf[T]
    }
  }
  */

  def createLayerStack(layerNames: Array[String])(implicit commonZoom: Int, catalogPath: String/*attributeStore: HadoopAttributeStore, layerReader: HadoopLayerReader*/, sc: SparkContext): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
    var tilesmerged : RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = null
    layerNames.foreach( layerName => {
      logger info s"Reading Layer $layerName"
      if (biggis.landuse.api.layerExists(LayerId(layerName, commonZoom))/*attributeStore.layerExists(layerName,commonZoom)*/) {
        //var tiles: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = layerReaderMB(getLayerId(layerName))(layerReader)
        //val tiles: SpatialMultibandRDD = biggis.landuse.api.readRddFromLayerT[SpatialMultibandRDD]((layerName, commonZoom))
        val tiles: SpatialMultibandRDD = biggis.landuse.api.readRddFromLayer[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]]((layerName, commonZoom))
        if (tilesmerged == null) {
          tilesmerged = tiles
        } else {
          if(tilesmerged.metadata.crs != tiles.metadata.crs){
            logger info s"Mismatch crs: ${tilesmerged.metadata.crs.proj4jCrs.getDatum.getName} != ${tiles.metadata.crs.proj4jCrs.getDatum.getName} "
            val (zoomlevel, tilesreproj) : (Int, SpatialMultibandRDD) = tiles.reproject(destCrs = tilesmerged.metadata.crs, layoutDefinition = tilesmerged.metadata.layout)
            logger info s"Reproject crs: ${tiles.metadata.crs.proj4jCrs.getDatum.getName} to ${tilesmerged.metadata.crs.proj4jCrs.getDatum.getName} "
            tilesmerged = stack2MBlayers(tilesmerged, tilesreproj)
          }
          else
            tilesmerged = stack2MBlayers(tilesmerged, tiles)
        }
      }
      else {
        logger info s"Layer not found: $layerName"
        throw new RuntimeException(s"Layer not found : $layerName")
      }
    })
    tilesmerged
  }
}
