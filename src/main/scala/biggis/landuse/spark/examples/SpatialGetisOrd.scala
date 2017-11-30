package biggis.landuse.spark.examples

import biggis.landuse.api.SpatialRDD
import biggis.landuse.api.catalogToStore
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster.summary.Statistics
import geotrellis.raster.withTileMethods
import geotrellis.spark.LayerId
import geotrellis.spark.Metadata
import geotrellis.spark.SpatialKey
import geotrellis.spark.TileLayerMetadata
import geotrellis.spark.io.SpatialKeyFormat
import geotrellis.spark.io.hadoop.HadoopLayerReader
import geotrellis.spark.io.spatialKeyAvroFormat
import geotrellis.spark.io.tileLayerMetadataFormat
import geotrellis.spark.io.tileUnionCodec
import org.apache.spark.rdd.RDD

object SpatialGetisOrd extends App with LazyLogging {

  val layerName = "morning2"
  val circleKernelRadius = 7

  implicit val catalogPath = "target/geotrellis-catalog"
  implicit val sc = Utils.initSparkAutoContext

  val maybeLayerId = biggis.landuse.api.getMaxZoomLevel(layerName)

  maybeLayerId match {
    case None => logger error s"Layer '$layerName' not found in the catalog '$catalogPath'"
    case Some(layerId) => {
      logger debug s"The following layerId will be used: $layerId"

      val layerReader = HadoopLayerReader(catalogPath)

      val queryResult: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
        layerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)

      val weightMatrix = Kernel.circle(circleKernelRadius, queryResult.metadata.cellwidth, circleKernelRadius)
      logger info s"extent of weightMatrix is ${weightMatrix.extent}"

      val stats = queryResult.histogram.statistics
      require(stats.nonEmpty)

      val Statistics(_, globMean, _, _, globStdev, _, _) = stats.get
      val numPixels = queryResult.histogram.totalCount
      logger info s"GLOBAL MEAN:  ${globMean}"
      logger info s"GLOBAL STDEV: ${globStdev}"
      logger info s"GLOBAL NUMPX: ${numPixels}"

      val outRdd = getisord(queryResult, weightMatrix, globMean, globStdev, numPixels )

      // this will be the new convoluted layer
      val convolvedLayerId = LayerId(layerId.name + "_gstar", layerId.zoom)

      biggis.landuse.api.deleteLayerFromCatalog(convolvedLayerId)
      biggis.landuse.api.writeRddToLayer(outRdd, convolvedLayerId)
    }
  }

  def getisord(rdd: SpatialRDD, weightMatrix: Kernel,
               globalMean:Double, globalStdev:Double, numPixels:Long): SpatialRDD = {

    val wcells = weightMatrix.tile.toArrayDouble
    val sumW = wcells.sum
    val sumW2 = wcells.map(x => x*x).sum
    val A = globalMean * sumW
    val B = globalStdev * Math.sqrt( (numPixels * sumW2 - sumW*sumW) / (numPixels - 1) )

    rdd.withContext {
      _.bufferTiles(weightMatrix.extent)
        .mapValues { tileWithCtx =>
          tileWithCtx.tile
            .focalSum(weightMatrix, Some(tileWithCtx.targetArea))
            .mapDouble { x => (x - A) / B }
        }
    }
  }
}
