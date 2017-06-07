package biggis.landuse.spark.examples

import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.{Tile, withTileMethods}
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io.file.{FileAttributeStore, FileLayerManager, FileLayerReader, FileLayerWriter}
import geotrellis.spark.io.{LayerHeader, SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import geotrellis.raster.{io => _, _}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, _}
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerReader}
import geotrellis.spark.stitch._
import geotrellis.spark.tiling.{FloatingLayoutScheme, LayoutDefinition, ZoomedLayoutScheme}
import geotrellis.spark.{io => _, _}
import geotrellis.vector.Extent
import org.apache.hadoop.fs.Path

// https://github.com/geotrellis/geotrellis/blob/master/docs/spark/spark-examples.md

@deprecated("for debugging only (attention: writes many tiles, not single file)")
object MultibandLayerToGeotiff extends LazyLogging{
  def main(args: Array[String]): Unit = {
    try {
      val Array(layerName, outputPath, catalogPath) = args
      implicit val sc = Utils.initSparkContext  // do not use - only for dirty debugging
      MultibandLayerToGeotiff(layerName, outputPath)(catalogPath, sc)
      sc.stop()
      logger debug "Spark context stopped"
    } catch {
      case _: MatchError => println("Run as: layerName outputPath /path/to/catalog")
    }
  }

  def apply(layerName: String, outputPath: String)(implicit catalogPath: String, sc: SparkContext): Unit = {
    logger info s"Writing layer '$layerName' in catalog '$catalogPath' to '$outputPath'"

    //implicit val sc = Utils.initSparkContext

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

    /*
    // ToDo: check if RDD is Tile or MultibandTile
    val (srcLayerMetadata, srcLayerSchema) =
    try {
      (
        layerReader.attributeStore
          .readMetadata(id = srcLayerId) ,
        layerReader.attributeStore
          //.readSchema(id = srcLayerId)  //.getFields()
            .readAll(layerName)
      )
    } catch {
      case _: Throwable =>
    }
    */
    val header = layerReader.attributeStore.readHeader[LayerHeader](srcLayerId)
    //assert(header.keyClass == "geotrellis.spark.SpatialKey")
    //assert(header.valueClass == "geotrellis.raster.Tile")

    val inputRdd:RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      if(header.valueClass == "geotrellis.raster.Tile") {
        layerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](srcLayerId)
          .withContext { rdd =>
            rdd.map { case (spatialKey, tile) => (spatialKey, ArrayMultibandTile(tile)) }
          }
      }
      else {
        assert(header.valueClass == "geotrellis.raster.MultibandTile")
        layerReader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](srcLayerId)
      }


    val metadata = inputRdd.metadata
    val crs = metadata.crs

    // test re-tile
    /*
    val myTILE_SIZE = 1024 //256 //Utils.TILE_SIZE
    val myRDD_PARTITIONS = 32 //32 //Utils.RDD_PARTITIONS
    val myRESAMPLING_METHOD = geotrellis.raster.resample.NearestNeighbor //Bilinear //Utils.RESAMPLING_METHOD

    val layout = {
      //val layoutScheme = FloatingLayoutScheme(tileSize = myTILE_SIZE)  // Utils.TILE_SIZE
      //val layoutScheme = ZoomedLayoutScheme(crs, tileSize = myTILE_SIZE) // Utils.TILE_SIZE
      val layoutTile = TileLayout(metadata.layout.layoutCols, metadata.layout.layoutRows, myTILE_SIZE, myTILE_SIZE )
      LayoutDefinition(extent = metadata.extent, layoutTile)
    }

    val myMetadata = TileLayerMetadata(
      metadata.cellType,
      layout,   //metadata.layout,
      metadata.extent,
      metadata.crs,
      metadata.bounds)
    // */

    // ToDo: replace "256x256 tiles" by "intelligent" tile size (as many as necessary, as few as possible)
    val outputRdd:RDD[(SpatialKey, MultibandTile)] = inputRdd
      //.tileToLayout(metadata.cellType, metadata.layout, Utils.RESAMPLING_METHOD)
      //.repartition(Utils.RDD_PARTITIONS)
      //.repartition(myRDD_PARTITIONS)
      //.tileToLayout(myMetadata.cellType, myMetadata.layout, myRESAMPLING_METHOD)

      outputRdd.foreach (mbtile => {
        val (key, tile) = mbtile
        val (col, row) = (key.col, key.row)
        val tileextent: Extent = metadata.layout.mapTransform(key)
      MultibandGeoTiff(tile, tileextent, crs)
        .write(outputPath + "_" + col + "_" + row + ".tif")
    }
    )

    ////val raster: Raster[MultibandTile] = tile.reproject(metadata.extent, metadata.crs, metadata.crs)
    //MultibandGeoTiff(tile, metadata.extent, crs).write(outputPath)

    //sc.stop()
    //logger debug "Spark context stopped"

    logger info "done."
  }
}
