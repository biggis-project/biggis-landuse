package biggis.landuse.spark.examples

import biggis.landuse.spark.examples.ManySingleBandLayersToMultibandLayer.logger
import biggis.landuse.spark.examples.UtilsSVM.LabelPointSpatialRef
import com.typesafe.scalalogging.slf4j.LazyLogging
import geotrellis.raster.io.HistogramDoubleFormat
import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster.{DoubleArrayTile, MultibandTile, Tile, withTileMethods}
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerReader, HadoopLayerWriter}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod.spatialKeyIndexMethod
import geotrellis.spark.io.{SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._
import org.apache.spark.rdd._

object TilePixelingExample extends LazyLogging {

  def main(args: Array[String]): Unit = {
    try {
      val Array(layerName, catalogPath) = args
      TilePixelingExample(layerName)(catalogPath)
    } catch {
      case _: MatchError => println("Run as: layerName /path/to/catalog")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
    }
  }

  def apply(layerName: String)(implicit catalogPath: String): Unit = {
    logger info s"Running convolution of layer '$layerName' in catalog '$catalogPath'"

    implicit val sc = Utils.initSparkContext

    // Create the attributes store that will tell us information about our catalog.
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

    //val queryResult: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = layerReader
    //  .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](srcLayerId)

    //For image layers we need multiband
    val queryResult: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = layerReader
      .read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](srcLayerId)

    // TODO: adjust this code to produce stream of pixels from each tile (flatmap)
    //    val convolvedLayerRdd = queryResult
    //        .flatMapValues { v =>
    //          // TODO: convert tile to pixels here
    //        }
    //    }

    // Note: expecially for MLLib SVM we could also use LabeledPoint format (but needs to be generalized for other cases):
    //val (data, data_spatialref) = UtilsSVM.MultibandTile2LabelPoint(queryResult)
    //val normalizer = new Normalizer(p = 255.0)
    //val data_norm : RDD[LabeledPoint] = data.map(x => LabeledPoint(x.label, normalizer.transform(x.features)))
    //UtilsSVM.SaveAsLibSVMFile(data_norm, "data/normalized_training_data.csv")

    // Test using LabelPoint.features (equals org.apache.spark.mllib.linalg.Vector)
    //    val pixelRdd = queryResult
    //      .flatMap { tile =>
    //        val labeledPoints = UtilsSVM.MultibandTile2LabelPointWithClassNo(tile, 0)
    //        labeledPoints.map { case(labeledPoint, ref) =>
    //          (ref.spatialKey, ref.offset, labeledPoint.features)
    //        }
    //      }
    //
    //    implicit val myOrdering = new Ordering[(SpatialKey, Int, org.apache.spark.mllib.linalg.Vector)] {
    //      override def compare(a: (SpatialKey, Int, org.apache.spark.mllib.linalg.Vector), b: (SpatialKey, Int, org.apache.spark.mllib.linalg.Vector)) =
    //        0
    //    }
    //    pixelRdd.top(10).foreach{ case(key, offset, vector) =>
    //        println(s"$key $offset : $vector")
    //    }

    val samples: RDD[(SpatialKey, (Int, Int, LabeledPoint))] with Metadata[TileLayerMetadata[SpatialKey]] =
      queryResult.withContext { rdd =>
        rdd.flatMapValues(mbtile =>
          UtilsML.MultibandTile2LabeledPixelSamples(mbtile, classBandNo = 0)
        )
      }

    val outTiles: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      samples.withContext { rdd =>
        rdd.groupByKey().map { case (spatialKey, listOfPixels) =>
          val arr = Array.ofDim[Double](256 * 256)
          listOfPixels.foreach { case (x, y, lp) =>
            arr(x + y * 256) = lp.label
          }

          (spatialKey, DoubleArrayTile(arr, 256, 256))
        }
      }

    // Create the writer that we will use to store the tiles in the local catalog.
    val writer = HadoopLayerWriter(catalogPathHdfs, attributeStore)
    val layerIdOut = LayerId("TODO_outlayer", srcLayerId.zoom )// TODO:srcLayerId.zoom

    // If the layer exists already, delete it out before writing
    if (attributeStore.layerExists(layerIdOut)) {
      logger debug s"Layer $layerIdOut already exists, deleting ..."
      HadoopLayerDeleter(attributeStore).delete(layerIdOut)
    }

    logger debug "Writing reprojected tiles using space filling curve"
    writer.write(layerIdOut, outTiles, ZCurveKeyIndexMethod)

    sc.stop()
    logger info "done."
  }
}
