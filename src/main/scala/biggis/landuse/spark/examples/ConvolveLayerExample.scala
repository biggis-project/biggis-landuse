package biggis.landuse.spark.examples

import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster.withTileMethods
import geotrellis.spark.LayerId
import geotrellis.spark.Metadata
import geotrellis.spark.SpatialKey
import geotrellis.spark.TileLayerMetadata
import geotrellis.spark.io.SpatialKeyFormat
import geotrellis.spark.io.hadoop.HadoopAttributeStore
import geotrellis.spark.io.hadoop.HadoopLayerReader
import geotrellis.spark.io.spatialKeyAvroFormat
import geotrellis.spark.io.tileLayerMetadataFormat
import geotrellis.spark.io.tileUnionCodec
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.rdd.RDD

object ConvolveLayerExample extends LazyLogging {

  def main(args: Array[String]): Unit = {
    try {
      val Array(layerName, circleKernelRadius, catalogPath) = args
      implicit val sc = Utils.initSparkAutoContext
      ConvolveLayerExample(layerName, circleKernelRadius.toInt)(catalogPath, sc)
      sc.stop()
    } catch {
      case _: MatchError => println("Run as: layerName circleKernelRadius /path/to/catalog")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
    }
  }

  def apply(layerName: String, circleKernelRadius: Int)(implicit catalogPath: String, sc: SparkContext): Unit = {
    logger info s"Running convolution of layer '$layerName' in catalog '$catalogPath'"
    logger info s"Using circular kernel of radius $circleKernelRadius"

    //implicit val sc = Utils.initSparkContext

    // Create the attributes store that will tell us information about our catalog.
    val catalogPathHdfs = new Path(catalogPath)
    val attributeStore = HadoopAttributeStore(catalogPathHdfs)
    val layerReader = HadoopLayerReader(attributeStore)

    val zoomsOfLayer = attributeStore.layerIds filter (_.name == layerName)
    if (zoomsOfLayer.isEmpty) {
      logger error s"Layer '$layerName' not found in the catalog '$catalogPath'"
      return
    }

    val srcLayerId = zoomsOfLayer.sortBy(_.zoom).last
    logger debug s"The following layerId will be used: $srcLayerId"

    val queryResult: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = layerReader
      .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](srcLayerId)

    val focalKernel = Kernel.circle(circleKernelRadius, queryResult.metadata.cellwidth, circleKernelRadius)
    logger info s"extent of focalKernel is ${focalKernel.extent}"

    // here, the convolution takes place
    // see also https://media.readthedocs.org/pdf/geotrellis/stable/geotrellis.pdf
    val convolvedLayerRdd = queryResult.withContext { rdd =>
      rdd
        .bufferTiles(focalKernel.extent)
        .mapValues { v =>
          v.tile.focalMean(focalKernel, Some(v.targetArea))
        }
    }

    // this will be the new convoluted layer
    val convolvedLayerId = LayerId(srcLayerId.name + "_conv", srcLayerId.zoom)

    biggis.landuse.api.deleteLayerFromCatalog(convolvedLayerId)
    biggis.landuse.api.writeRddToLayer(convolvedLayerRdd, convolvedLayerId)

    //sc.stop()
    logger info "done."
  }
}
