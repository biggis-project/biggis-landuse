package biggis.landuse.spark.examples

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{SparkContext, SparkException}
import biggis.landuse.api._
import geotrellis.spark.LayerId

object LayerUpdaterExample extends LazyLogging {  //extends App with LazyLogging
  def main(args: Array[String]): Unit = {
    try {
      val Array(layerNameToUpdate, layerNameNew, catalogPath) = args
      implicit val sc : SparkContext = Utils.initSparkAutoContext  // only for debugging - needs rework
      LayerUpdaterExample(layerNameToUpdate, layerNameNew)(catalogPath,sc)
      sc.stop()
      logger debug "Spark context stopped"
    } catch {
      case _: MatchError => println("Run as: inputLayerName1 inputLayerName2 [inputLayerName3 ...] layerStackNameOut /path/to/catalog")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parameter: -Dspark.master=local[*]"
    }
  }

  def apply(layerNameToUpdate: String, layerNameNew: String)(implicit catalogPath: String, sc: SparkContext): Unit = {

    logger info s"Updating '$layerNameToUpdate' from '$layerNameNew' inside '$catalogPath'"

    /*
    //val updater = HadoopLayerUpdater(hdfsPath)
    val writer = HadoopLayerWriter(hdfsPath)
    val reader = HadoopLayerReader(hdfsPath)

    // Pyramiding up the zoom levels, write our tiles out to the local file system.
    Pyramid.upLevels(reprojected, layoutScheme, zoom, CubicConvolution) { (rdd, zoomlevel) =>
      val layerId = LayerId("layer_sat", zoomlevel)
      val existing = reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)

      // If the layer exists already, update it
      if(attributeStore.layerExists(layerId)) {
        //updater.update(layerId, existing.merge(rdd))  //deprecated
        writer.overwrite(layerId, existing.merge(rdd))  //better use new writer.overwrite
      } else {
        writer.write(layerId, rdd, ZCurveKeyIndexMethod)
      }
      */

    // ToDo: Delete after testing - Only for testing create layerToUpdate from "layer_sat1" for testing
    /*
    val tempLayerId = getMaxZoomLevel("layer_sat1").get
    val rddTemp : SpatialMultibandRDD = readRddFromLayer(getMaxZoomLevel("layer_sat1").get)
    writeRddToLayer(rddTemp,LayerId(layerNameToUpdate,tempLayerId.zoom))
    */

    // Update layerNameToUpdate with layerNameNew
    mergeRddIntoLayer(readRddFromLayer(getMaxZoomLevel(layerNameNew).get) : SpatialMultibandRDD,getMaxZoomLevel(layerNameToUpdate).get)
  }

}
