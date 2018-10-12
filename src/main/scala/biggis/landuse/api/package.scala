package biggis.landuse

import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.ArrayMultibandTile
import geotrellis.raster.MultibandTile
import geotrellis.raster.Tile
import geotrellis.raster.io.HistogramDoubleFormat
import geotrellis.spark.LayerId
import geotrellis.spark.Metadata
import geotrellis.spark.SpaceTimeKey
import geotrellis.spark.SpatialKey
import geotrellis.spark.TileLayerMetadata
import geotrellis.spark.io.LayerHeader
import geotrellis.spark.io.hadoop.HadoopAttributeStore
import geotrellis.spark.io.hadoop.HadoopLayerDeleter
import geotrellis.spark.io.hadoop.HadoopLayerWriter
import geotrellis.spark.io.hadoop.HadoopLayerReader
import geotrellis.spark.io.index.HilbertKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.json.Implicits._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.RemoteIterator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.language.implicitConversions
import scala.reflect.runtime.universe._

/**
  * Created by Viliam Simko (viliam.simko@gmail.com)
  */
package object api extends LazyLogging {

  type SpatialRDD = RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]  //TileLayerRDD[SpatialKey]
  type SpaceTimeRDD = RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]  //TileLayerRDD[SpaceTimeKey]
  type SpatialMultibandRDD = RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]  //MultibandTileLayerRDD[SpatialKey]
  type SpaceTimeMultibandRDD = RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]  //MultibandTileLayerRDD[SpaceTimeKey]

  /**
    * Converts SparkSession to SparkContext
    * */
  implicit def sessionToContext(spark: SparkSession): SparkContext = { spark.sparkContext }

  /**
    * Converts RemoteIterator from Hadoop to Scala Iterator that provides all the familiar functions such as map,
    * filter, foreach, etc.
    *
    * @param underlying The RemoteIterator that needs to be wrapped
    * @tparam T Items inside the iterator
    * @return Standard Scala Iterator
    */
  implicit def convertToScalaIterator[T](underlying: RemoteIterator[T]): Iterator[T] = {
    case class wrapper(underlying: RemoteIterator[T]) extends Iterator[T] {
      override def hasNext: Boolean = underlying.hasNext

      override def next: T = underlying.next
    }
    wrapper(underlying)
  }

  implicit def catalogToStore(catalogPath: String)(implicit sc: SparkContext): HadoopAttributeStore = {
    val hdfsPath = new Path(catalogPath)
    HadoopAttributeStore(hdfsPath)
  }

  def deleteLayerFromCatalog(layerId: LayerId)(implicit catalogPath: String, sc: SparkContext): Unit = {
    deleteLayerFromCatalog(layerId.name)
  }

  /**
    * Unconditionally deletes a raster layer (including all zoom levels) from a geotrellis catalog.
    * Does not complain if the layer does not exist.
    */
  def deleteLayerFromCatalog(layerName: String)(implicit catalogPath: String, sc: SparkContext): Unit = {

    val store = catalogToStore(catalogPath)

    val deleter = HadoopLayerDeleter(catalogPath)
    deleter.attributeStore.layerIds filter (_.name == layerName) foreach deleter.delete

    // try to delete the directory if it still exists
    // we do this because geotrellis leaves an empty directory behind
    // we could delete this step once geotrellis implementation is fixed
    val layerPath = store.rootPath.suffix(s"/$layerName")
    if(store.fs.exists(layerPath))
      store.fs.delete(layerPath, true)
  }

  /**
    * Unconditionally deletes a single zoom level within a raster layer (inside a geotrellis catalog).
    * Does not complain if the zoom level does not exist.
    */
  def deleteZoomLevelFromLayer(layerName: String, zoomLevel: Int)
                              (implicit catalogPath: String, sc: SparkContext): Unit = {
    val deleter = HadoopLayerDeleter(catalogPath)

    deleter.attributeStore.layerIds filter (_.name == layerName) filter (_.zoom == zoomLevel) foreach { layerId =>
      deleter.delete(layerId)
      logger debug s"Deleted $layerId"
    }
  }

  /**
    * Pure function.
    * Finds layerId which represents the highest zoom level.
    */
  def getMaxZoomLevel(layerName: String)
                     (implicit catalogPath: String, sc: SparkContext): Option[LayerId] = {

    val catalogPathHdfs = new Path(catalogPath)
    val attributeStore = HadoopAttributeStore(catalogPathHdfs)

    val zoomsOfLayer = attributeStore.layerIds filter (_.name == layerName)
    if (zoomsOfLayer.isEmpty) {
      return None
    }

    val layerId = zoomsOfLayer.maxBy(_.zoom)

    Some(layerId)
  }

  /**
    * @param rdd         The RDD representing a processed layer of tiles
    * @param layerId     layerName and zoom level
    * @param catalogPath Geotrellis catalog
    * @param sc          SparkContext
    */
  def writeRddToLayer[K, V, M]
    (rdd: RDD[(K, V)] with Metadata[M], layerId: LayerId)
    (implicit catalogPath: String, sc: SparkContext, ttagKey: TypeTag[K], ttagValue: TypeTag[V], ttagMeta: TypeTag[M]): Unit = {

    logger debug s"Writing RDD to layer '${layerId.name}' at zoom level ${layerId.zoom} ..."

    val writer = HadoopLayerWriter(new Path(catalogPath))

    // TODO: This code is really nasty, there must be a better way !!!
    if (ttagKey.tpe =:= typeOf[SpatialKey] && ttagValue.tpe =:= typeOf[Tile] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpatialKey]]) {

      logger debug s"Writing using SpatialKey + ZCurveKeyIndexMethod + Tile ..."
      val rdd2 = rdd.asInstanceOf[SpatialRDD]
      writer.write(layerId, rdd2, ZCurveKeyIndexMethod)

      logger debug s"Writing histogram of layer '${layerId.name}' to attribute store as 'histogramData' for zoom level 0"
      writer.attributeStore.write(LayerId(layerId.name, 0), "histogramData", rdd2.histogram)

    } else if (ttagKey.tpe =:= typeOf[SpaceTimeKey] && ttagValue.tpe =:= typeOf[Tile] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpaceTimeKey]]) {

      logger debug s"Writing using SpaceTimeKey + HilbertKeyIndexMethod + Tile ..."
      val rdd2 = rdd.asInstanceOf[RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]]
      writer.write(layerId, rdd2, HilbertKeyIndexMethod(1))

    } else if (ttagKey.tpe =:= typeOf[SpatialKey] && ttagValue.tpe =:= typeOf[MultibandTile] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpatialKey]]) {

      logger debug s"Writing using SpatialKey + ZCurveKeyIndexMethod + MultibandTile ..."
      val rdd2 = rdd.asInstanceOf[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]]
      writer.write(layerId, rdd2, ZCurveKeyIndexMethod)

    } else if (ttagKey.tpe =:= typeOf[SpaceTimeKey] && ttagValue.tpe =:= typeOf[MultibandTile] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpaceTimeKey]]) {

      logger debug s"Writing using SpaceTimeKey + HilbertKeyIndexMethod + MultibandTile ..."
      val rdd2 = rdd.asInstanceOf[RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]]
      writer.write(layerId, rdd2, HilbertKeyIndexMethod(1))

    } else if ((ttagKey.tpe =:= typeOf[SpatialKey] && !(ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpatialKey]]))
      || (ttagKey.tpe =:= typeOf[SpaceTimeKey] && !(ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpaceTimeKey]]))) {
      throw new RuntimeException("we did not expect any other key with meta combination than SpatialKey with TileLayerMetadata[SpatialKey] or SpaceTimeKey with TileLayerMetadata[SpaceTimeKey] ")
    } else if (!(ttagValue.tpe =:= typeOf[Tile]) && !(ttagValue.tpe =:= typeOf[MultibandTile])
      && !(ttagKey.tpe =:= typeOf[SpatialKey] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpatialKey]])
      && !(ttagKey.tpe =:= typeOf[SpaceTimeKey] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpaceTimeKey]])) {
      throw new RuntimeException("we did not expect any other key type than SpatialKey or SpaceTimeKey and any other tile type than Tile or MultibandTile")
    } else if (!(ttagValue.tpe =:= typeOf[Tile]) && !(ttagValue.tpe =:= typeOf[MultibandTile])) {
      throw new RuntimeException("we did not expect any other type than Tile or MultibandTile")
    } else {
      throw new RuntimeException("we did not expect any other type than SpatialKey or SpaceTimeKey")
    }

    logger debug s"Writing done..."
  }

  /**
    * @param layerId     layerName and zoom level
    * @param bandNumber  Optional: select specific band number from layer (only applies to reading MultibandTile as Tile, ignored otherwise), defaults to 0 (first band) if None
    * @param catalogPath Geotrellis catalog
    * @param sc          SparkContext
    * @return            RDD[(K, V)] with Metadata[M] representing a layer of tiles
    */
  def readRddFromLayer[K, V, M]
  (layerId: LayerId, bandNumber : Option[Int] = None : Option[Int])
  (implicit catalogPath: String, sc: SparkContext, ttagKey: TypeTag[K], ttagValue: TypeTag[V], ttagMeta: TypeTag[M]): RDD[(K, V)] with Metadata[M] = {

    logger debug s"Reading RDD from layer '${layerId.name}' at zoom level ${layerId.zoom} ..."

    val reader = HadoopLayerReader(new Path(catalogPath))

    if(!reader.attributeStore.layerExists(layerId)){
      logger error s"Layer '${layerId.name}' not found (at zoom level ${layerId.zoom}) ..."
      throw new RuntimeException(s"Error: Layer '${layerId.name}' not found (at zoom level ${layerId.zoom}) ...")
    }

    val rdd : RDD[(K, V)] with Metadata[M] =
    if (ttagKey.tpe =:= typeOf[SpatialKey] && ttagValue.tpe =:= typeOf[Tile] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpatialKey]]) {

      logger debug s"Reading using SpatialKey + Tile ..." // (+ ZCurveKeyIndexMethod)
      val rdd : RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
        try {
          val header = reader.attributeStore.readHeader[LayerHeader](layerId)
          assert(header.keyClass == "geotrellis.spark.SpatialKey")
          if (header.valueClass == "geotrellis.raster.MultibandTile"){
            val bandNo = bandNumber getOrElse 0 //Optional: select specific band number from layer, default: 0 (first band)
            reader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](layerId)
              .withContext { rdd =>
                rdd.map { case (spatialKey, tile) => (spatialKey, tile.band(bandNo)) } // for Tile read only first band of MultibandTile
              }
          }
          else {
            assert(header.valueClass == "geotrellis.raster.Tile")
            reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
          }
        }
        catch { case _: Throwable => null }
      rdd.asInstanceOf[RDD[(K, V)] with Metadata[M]]

    } else if (ttagKey.tpe =:= typeOf[SpaceTimeKey]&& ttagValue.tpe =:= typeOf[Tile] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpaceTimeKey]]) {

      logger debug s"Reading using SpaceTimeKey + Tile ..." // (+ HilbertKeyIndexMethod)
      val rdd : RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] =
        try {
          val header = reader.attributeStore.readHeader[LayerHeader](layerId)
          assert(header.keyClass == "geotrellis.spark.SpaceTimeKey")
          if (header.valueClass == "geotrellis.raster.MultibandTile"){
            val bandNo = bandNumber getOrElse 0 //Optional: select specific band number from layer, default: 0 (first band)
            reader.read[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](layerId)
              .withContext { rdd =>
                rdd.map { case (spatialKey, tile) => (spatialKey, tile.band(bandNo)) } // for Tile read only first band of MultibandTile
              }
          }
          else {
            assert(header.valueClass == "geotrellis.raster.Tile")
            reader.read[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](layerId)
          }
        }
        catch { case _: Throwable => null }
      rdd.asInstanceOf[RDD[(K, V)] with Metadata[M]]

    } else if(ttagKey.tpe =:= typeOf[SpatialKey] && ttagValue.tpe =:= typeOf[MultibandTile] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpatialKey]]) {

      logger debug s"Reading using SpatialKey + MultibandTile ..."  // (+ ZCurveKeyIndexMethod)
      val rdd : RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] =
        try {
          val header = reader.attributeStore.readHeader[LayerHeader](layerId)
          assert(header.keyClass == "geotrellis.spark.SpatialKey")
          if (header.valueClass == "geotrellis.raster.Tile"){
            reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
              .withContext { rdd =>
                rdd.map { case (spatialKey, tile) => (spatialKey, ArrayMultibandTile(tile)) } // for MultibandTile read single band of Tile wrapped by ArrayMultibandTile
              }
          }
          else {
            assert(header.valueClass == "geotrellis.raster.MultibandTile")
            reader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](layerId)
          }
        }
        catch { case _: Throwable => null }
      rdd.asInstanceOf[RDD[(K, V)] with Metadata[M]]

    } else if(ttagKey.tpe =:= typeOf[SpaceTimeKey] && ttagValue.tpe =:= typeOf[MultibandTile] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpaceTimeKey]]) {

      logger debug s"Reading using SpaceTimeKey + MultibandTile ..."  // (+ HilbertKeyIndexMethod)
      val rdd : RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] =
        try {
          val header = reader.attributeStore.readHeader[LayerHeader](layerId)
          assert(header.keyClass == "geotrellis.spark.SpaceTimeKey")
          if (header.valueClass == "geotrellis.raster.Tile"){
            reader.read[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](layerId)
              .withContext { rdd =>
                rdd.map { case (spatialKey, tile) => (spatialKey, ArrayMultibandTile(tile)) } // for MultibandTile read single band of Tile wrapped by ArrayMultibandTile
              }
          }
          else {
            assert(header.valueClass == "geotrellis.raster.MultibandTile")
            reader.read[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](layerId)
          }
        }
        catch { case _: Throwable => null }
      rdd.asInstanceOf[RDD[(K, V)] with Metadata[M]]

    } else {
      val rdd : RDD[(K, V)] with Metadata[M] = sc.emptyRDD[(K, V)].asInstanceOf[RDD[(K, V)] with Metadata[M]]
      if ((ttagKey.tpe =:= typeOf[SpatialKey] && !(ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpatialKey]]))
        || (ttagKey.tpe =:= typeOf[SpaceTimeKey] && !(ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpaceTimeKey]]))) {
        throw new RuntimeException("we did not expect any other key with meta combination than SpatialKey with TileLayerMetadata[SpatialKey] or SpaceTimeKey with TileLayerMetadata[SpaceTimeKey] ")
      } else if (!(ttagValue.tpe =:= typeOf[Tile]) && !(ttagValue.tpe =:= typeOf[MultibandTile])
        && !(ttagKey.tpe =:= typeOf[SpatialKey] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpatialKey]])
        && !(ttagKey.tpe =:= typeOf[SpaceTimeKey] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpaceTimeKey]])) {
        throw new RuntimeException("we did not expect any other key type than SpatialKey or SpaceTimeKey and any other tile type than Tile or MultibandTile")
      } else if (!(ttagValue.tpe =:= typeOf[Tile]) && !(ttagValue.tpe =:= typeOf[MultibandTile])) {
        throw new RuntimeException("we did not expect any other type than Tile or MultibandTile")
      } else if (!(ttagKey.tpe =:= typeOf[SpatialKey]) && !(ttagKey.tpe =:= typeOf[SpaceTimeKey])){
        throw new RuntimeException("we did not expect any other type than SpatialKey or SpaceTimeKey")
      }
      rdd
    }
    logger debug s"Reading done..."

    rdd
  }

  /**
    * Checks if the layer / zoom level does exist
    * @param layerId     layerName and zoom level
    * @param catalogPath Geotrellis catalog
    * @param sc          SparkContext
    * @return            Boolean true if layer exists, false otherwise
    */
  def layerExists(layerId: LayerId)
                 (implicit catalogPath: String, sc: SparkContext) : Boolean = {
    HadoopLayerReader(new Path(catalogPath)).attributeStore.layerExists(layerId)
  }

  /**
    * @param rdd         The RDD representing a processed layer of tiles
    * @param layerId     layerName and zoom level
    * @param catalogPath Geotrellis catalog
    * @param sc          SparkContext
    */
  def mergeRddIntoLayer[K, V, M]
  (rdd: RDD[(K, V)] with Metadata[M], layerId: LayerId)
  (implicit catalogPath: String, sc: SparkContext, ttagKey: TypeTag[K], ttagValue: TypeTag[V], ttagMeta: TypeTag[M]): Unit = {

    logger debug s"Writing RDD to layer '${layerId.name}' at zoom level ${layerId.zoom} ..."

    // ToDo: check if Layer exists, check if ZoomLevel matches, if necessary use ZoomResampleLayer, add error handling

    val writer = HadoopLayerWriter(new Path(catalogPath))
    val reader = HadoopLayerReader(new Path(catalogPath))

    // TODO: This code is really nasty, there must be a better way !!!
    if (ttagKey.tpe =:= typeOf[SpatialKey] && ttagValue.tpe =:= typeOf[Tile] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpatialKey]]) {

      logger debug s"Updating using SpatialKey + ZCurveKeyIndexMethod + Tile ..."
      val rdd2 = rdd.asInstanceOf[SpatialRDD]
      val existing = reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
      writer.overwrite(layerId, existing.merge(rdd2)) //ZCurveKeyIndexMethod

      //logger debug s"Writing histogram of layer '${layerId.name}' to attribute store as 'histogramData' for zoom level 0"
      //writer.attributeStore.write(LayerId(layerId.name, 0), "histogramData", rdd2.histogram)

    } else if (ttagKey.tpe =:= typeOf[SpaceTimeKey] && ttagValue.tpe =:= typeOf[Tile] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpaceTimeKey]]) {

      logger debug s"Updating using SpaceTimeKey + HilbertKeyIndexMethod + Tile ..."
      val rdd2 = rdd.asInstanceOf[RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]]
      val existing = reader.read[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](layerId)
      writer.overwrite(layerId, existing.merge(rdd2)) //HilbertKeyIndexMethod(1)

    } else if (ttagKey.tpe =:= typeOf[SpatialKey] && ttagValue.tpe =:= typeOf[MultibandTile] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpatialKey]]) {

      logger debug s"Updating using SpatialKey + ZCurveKeyIndexMethod + MultibandTile ..."
      val rdd2 = rdd.asInstanceOf[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]]
      val existing = reader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](layerId)
      writer.overwrite(layerId, existing.merge(rdd2)) //ZCurveKeyIndexMethod

    } else if (ttagKey.tpe =:= typeOf[SpaceTimeKey] && ttagValue.tpe =:= typeOf[MultibandTile] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpaceTimeKey]]) {

      logger debug s"Updating using SpaceTimeKey + HilbertKeyIndexMethod + MultibandTile ..."
      val rdd2 = rdd.asInstanceOf[RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]]
      val existing = reader.read[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](layerId)
      writer.overwrite(layerId, existing.merge(rdd2)) //HilbertKeyIndexMethod(1)

    } else if ((ttagKey.tpe =:= typeOf[SpatialKey] && !(ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpatialKey]]))
      || (ttagKey.tpe =:= typeOf[SpaceTimeKey] && !(ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpaceTimeKey]]))) {
      throw new RuntimeException("we did not expect any other key with meta combination than SpatialKey with TileLayerMetadata[SpatialKey] or SpaceTimeKey with TileLayerMetadata[SpaceTimeKey] ")
    } else if (!(ttagValue.tpe =:= typeOf[Tile]) && !(ttagValue.tpe =:= typeOf[MultibandTile])
      && !(ttagKey.tpe =:= typeOf[SpatialKey] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpatialKey]])
      && !(ttagKey.tpe =:= typeOf[SpaceTimeKey] && ttagMeta.tpe =:= typeOf[TileLayerMetadata[SpaceTimeKey]])) {
      throw new RuntimeException("we did not expect any other key type than SpatialKey or SpaceTimeKey and any other tile type than Tile or MultibandTile")
    } else if (!(ttagValue.tpe =:= typeOf[Tile]) && !(ttagValue.tpe =:= typeOf[MultibandTile])) {
      throw new RuntimeException("we did not expect any other type than Tile or MultibandTile")
    } else {
      throw new RuntimeException("we did not expect any other type than SpatialKey or SpaceTimeKey")
    }

    logger debug s"Updating done..."
  }

  def initSparkSession: SparkSession = {
    logger info s"initSparkSession "
    // Attn.: Please set spark.master and spark.jars externally (e.g. via Spark Submit JSON)! It will be handled by SparkSession automatically
    SparkSession.builder
      .getOrCreate()
  }
}
