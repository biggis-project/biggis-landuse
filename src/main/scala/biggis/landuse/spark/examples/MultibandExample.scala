package biggis.landuse.spark.examples

import com.typesafe.scalalogging.StrictLogging
import geotrellis.proj4.WebMercator
import geotrellis.raster.io.HistogramDoubleFormat
import geotrellis.raster.io.geotiff.{GeoTiff, MultibandGeoTiff, reader}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{MultibandTile, MultibandRaster, Raster, Tile, withTileMethods}
import geotrellis.spark.io.file.{FileAttributeStore, FileLayerManager, FileLayerWriter}
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerReader, HadoopLayerWriter, HadoopSparkContextMethodsWrapper}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod.spatialKeyIndexMethod
import geotrellis.spark.io.{SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme}
import geotrellis.spark.{LayerId, Metadata, MultibandTileLayerRDD, SpatialKey, TileLayerMetadata, TileLayerRDD, withProjectedExtentTilerKeyMethods, withStatsTileRDDMethods, withTileRDDReprojectMethods, withTilerMethods}
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import geotrellis.spark.etl.hadoop

/**
  * Created by ak on 20.10.2016.
  */
@deprecated("do not use, only for debugging", "always")
object MultibandExample extends StrictLogging{

  def main(args: Array[String]): Unit = {
    try {
      val Array(inputPath, outputPath, layerName, catalogPath) = args
      MultibandExample(inputPath, outputPath, layerName)(catalogPath)
    } catch {
      case _: MatchError => println("Run as: inputPath outputPath layerName /path/to/catalog")
    }
  }

  def apply(inputPath: String, outputPath: String, layerName: String)(implicit catalogPath: String) {

    logger debug s"Building the pyramid '$layerName' from geotiff '$inputPath' ... "

    implicit val sc = Utils.initSparkContext

    // Multiband Read
    logger debug "Opening geotiff as RDD"
    val inputRdd = sc.hadoopMultibandGeoTiffRDD(inputPath)
    val (_, myRasterMetaData) = TileLayerMetadata.fromRDD(inputRdd, FloatingLayoutScheme(Utils.TILE_SIZE))

    val tiled = inputRdd
      .tileToLayout(myRasterMetaData.cellType, myRasterMetaData.layout, Utils.RESAMPLING_METHOD)
      .repartition(Utils.RDD_PARTITIONS)

    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = Utils.TILE_SIZE)

    logger debug "Reprojecting to WebMercator"
    val (zoom, reprojected) =
      MultibandTileLayerRDD(tiled, myRasterMetaData).reproject(WebMercator, layoutScheme, Utils.RESAMPLING_METHOD)

    // Create the attributes store that will tell us information about our catalog.
    val catalogPathHdfs = new Path(catalogPath)
    val attributeStore = HadoopAttributeStore(catalogPathHdfs)

    // Create the writer that we will use to store the tiles in the local catalog.
    val writer = HadoopLayerWriter(catalogPathHdfs, attributeStore)
    val layerId = LayerId(layerName, zoom)

    // If the layer exists already, delete it out before writing
    if (attributeStore.layerExists(layerId)) {
      logger debug s"Layer $layerId already exists, deleting ..."
      HadoopLayerDeleter(attributeStore).delete(layerId)
    }

    logger debug "Writing reprojected tiles using space filling curve"
    writer.write(layerId, reprojected, ZCurveKeyIndexMethod)

    //Utils.writeHistogram(attributeStore, layerName, reprojected.histogram)

    // Multiband Write
    val layerReader = HadoopLayerReader(attributeStore)

    val zoomsOfLayer = attributeStore.layerIds filter (_.name == layerName)
    if(zoomsOfLayer.isEmpty) {
      logger info s"Layer '$layerName' not found in the catalog '$catalogPath'"
      return
    }

    val srcLayerId = zoomsOfLayer.sortBy(_.zoom).last
    logger debug s"The following layerId will be used: $srcLayerId"

    val outputRdd:RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = layerReader
      .read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](srcLayerId)

    val metadata = outputRdd.metadata

    val tiled_out: RDD[(SpatialKey, MultibandTile)] = outputRdd.distinct()

    val tile: MultibandTile = tiled_out.stitch()

    val crs = metadata.crs

    MultibandGeoTiff(tile, metadata.extent, crs).write(outputPath)

    sc.stop()

  }

}