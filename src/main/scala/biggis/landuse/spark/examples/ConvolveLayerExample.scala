package biggis.landuse.spark.examples

import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.io.HistogramDoubleFormat
import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster.{Tile, withTileMethods}
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerReader, HadoopLayerWriter}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod.spatialKeyIndexMethod
import geotrellis.spark.io.{SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD

object ConvolveLayerExample extends LazyLogging {

  def main(args: Array[String]): Unit = {
    try {
      val Array(layerName, circleKernelRadius, catalogPath) = args
      ConvolveLayerExample(layerName, circleKernelRadius.toInt)(catalogPath)
    } catch {
      case _: MatchError => println("Run as: layerName circleKernelRadius /path/to/catalog")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
    }
  }

  def apply(layerName: String, circleKernelRadius: Int)(implicit catalogPath: String): Unit = {
    logger info s"Running convolution of layer '$layerName' in catalog '$catalogPath'"
    logger info s"Using circular kernel of radius $circleKernelRadius"

    implicit val sc = Utils.initSparkContext

    // Create the attributes store that will tell us information about our catalog.
    val catalogPathHdfs = new Path(catalogPath)
    val attributeStore = HadoopAttributeStore( catalogPathHdfs )
    val layerReader = HadoopLayerReader(attributeStore)

    val zoomsOfLayer = attributeStore.layerIds filter (_.name == layerName)
    if (zoomsOfLayer.isEmpty) {
      logger info s"Layer '$layerName' not found in the catalog '$catalogPath'"
      return
    }

    val srcLayerId = zoomsOfLayer.sortBy(_.zoom).last
    logger debug s"The following layerId will be used: $srcLayerId"

    val queryResult: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = layerReader
      .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](srcLayerId)

    val focalKernel = Kernel.circle(circleKernelRadius, queryResult.metadata.cellwidth, circleKernelRadius)

    // here, the convolution takes place
    val convolvedLayerRdd = queryResult.withContext { rdd =>
      rdd
        .bufferTiles(circleKernelRadius)
        .mapValues { v =>
          v.tile.focalMean(focalKernel)
        }
    }

    // this will be the new convoluted layer
    val convolvedLayerId = LayerId(srcLayerId.name + "_conv", srcLayerId.zoom)

    // automatically deleting existing layer
    if (attributeStore.layerExists(convolvedLayerId)) {
      logger debug s"Layer $convolvedLayerId already exists, deleting ..."
      HadoopLayerDeleter(attributeStore).delete(convolvedLayerId)
    }

    logger info s"Writing convoluted layer '${convolvedLayerId}'"
    val writer =  HadoopLayerWriter(catalogPathHdfs, attributeStore)
    writer.write(convolvedLayerId, convolvedLayerRdd, ZCurveKeyIndexMethod)

    Utils.writeHistogram(attributeStore, srcLayerId.name + "_conv", convolvedLayerRdd.histogram)

    sc.stop()
    logger info "done."
  }
}
