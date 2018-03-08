package biggis.landuse.spark.examples

import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.Tile
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.withTileMethods
import geotrellis.raster.{io => _}
import geotrellis.spark.Metadata
import geotrellis.spark.SpatialKey
import geotrellis.spark.TileLayerMetadata
import geotrellis.spark._
import geotrellis.spark.io.SpatialKeyFormat
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.hadoop.HadoopAttributeStore
import geotrellis.spark.io.hadoop.HadoopLayerReader
import geotrellis.spark.io.spatialKeyAvroFormat
import geotrellis.spark.io.tileLayerMetadataFormat
import geotrellis.spark.io.tileUnionCodec
import geotrellis.spark.{io => _}
import geotrellis.util._
import geotrellis.vector.Extent
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD

// https://github.com/geotrellis/geotrellis/blob/master/docs/spark/spark-examples.md

object LayerToGeotiff extends LazyLogging {
  def main(args: Array[String]): Unit = {
    try {
      implicit val sc = Utils.initSparkAutoContext
      val Array(layerName, outputPath, catalogPath) = args
      LayerToGeotiff(layerName, outputPath)(catalogPath, sc)
      sc.stop()
      logger debug "Spark context stopped"
    } catch {
      case _: MatchError => println("Run as: layerName outputPath /path/to/catalog")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parameter: -Dspark.master=local[*]"
    }
  }

  def apply(layerName: String, outputPath: String, useStitching: Boolean = false)(implicit catalogPath: String, sc: SparkContext): Unit = {
    logger info s"Writing layer '$layerName' in catalog '$catalogPath' to '$outputPath'"

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

    val metadata = inputRdd.metadata

    val crs = metadata.crs

    // Hadoop Config is accessible from SparkContext
    implicit val conf: Configuration = sc.hadoopConfiguration
    val serConf = new SerializableConfiguration(conf)
    //implicit val fs: FileSystem = FileSystem.get(conf);

    // ToDo: replace both "stitch" and "256x256 tiles" by "intelligent" tile size (as many as necessary, as few as possible)
    if (useStitching) { //Attn: stitched version may exceed max Memory, has georeference issues with WebMercator
      // one single GeoTiff, but attention
      val tiled: RDD[(SpatialKey, Tile)] = inputRdd.distinct()

      val tile: Tile = tiled.stitch()

      //val datum = crs.proj4jCrs.getDatum()
      //val epsg = crs.epsgCode.get
      //val param = crs.proj4jCrs.getParameters()
      //val proj = crs.proj4jCrs.getProjection()
      //val falseEasting = proj.getFalseEasting()
      if (crs.epsgCode.get == 3857) { //"WebMercator"
        val raster: Raster[Tile] = tile.reproject(metadata.extent, metadata.crs, metadata.crs)
        GeoTiff(raster, crs).write(outputPath)
        //val tileextent: Extent = metadata.extent
        //GeoTiff(tile, tileextent, crs).write(outputPath)
      } else {
        val layoutextent: Extent = metadata.layoutExtent
        GeoTiff(tile, layoutextent, crs).write(outputPath) //for UTM32
      }
    } else {
      // many GeoTiff tiles
      val outputRdd: RDD[(SpatialKey, Tile)] = inputRdd
      //.tileToLayout(metadata.cellType, metadata.layout, Utils.RESAMPLING_METHOD)
      //.repartition(Utils.RDD_PARTITIONS)

      val useSerializedHadoopConfig = true
      if(useSerializedHadoopConfig){
        // ToDo: test Spark Cluster version
        outputRdd.foreachPartition { partition =>
          partition.foreach { tuple =>
            val (key, tile) = tuple
            val (col, row) = (key.col, key.row)
            val tileextent: Extent = metadata.layout.mapTransform(key)
            val filename = new Path(outputPath + "_" + col + "_" + row + ".tif")
            logger info s" writing: '${filename.toString}'"
            GeoTiff(tile, tileextent, crs)
              .write(filename, serConf.value)
          }
        }
      } else {
        // only for local debugging - do not use in cloud // ToDo: delete after testing
        outputRdd.foreach(mbtile => {
          val (key, tile) = mbtile
          val (col, row) = (key.col, key.row)
          val tileextent: Extent = metadata.layout.mapTransform(key)
          //val filename = new Path(outputPath + "_" + col + "_" + row + ".tif")
          //logger info s" writing: '${filename.toString}'"
          GeoTiff(tile, tileextent, crs)
            //.write(filename.toString) //.write(filename, serConf.value)
            .write(outputPath + "_" + col + "_" + row + ".tif")
        }
        )
      }
    }

    //sc.stop()
    //logger debug "Spark context stopped"

    logger info "done."
  }
}
