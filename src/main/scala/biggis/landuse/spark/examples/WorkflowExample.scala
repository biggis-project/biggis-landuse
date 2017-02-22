package biggis.landuse.spark.examples

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.{SparkContext, SparkException}

/**
  * Created by ak on 15.02.2017.
  */
object WorkflowExample extends StrictLogging {
  def main(args: Array[String]): Unit = {
    try {
      val Array(catalogPath) = args
      //implicit val catalogPath = "target/geotrellis-catalog/"
      implicit val sc = Utils.initSparkContext
      WorkflowExample()(catalogPath, sc)
      sc.stop()
    }
    catch {
      case _: MatchError => println("Run as: /path/to/catalog")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
    }
  }

  def apply()(implicit catalogPath: String, sc: SparkContext): Unit = {
    // ToDo: generally replace SpatialKey by SpaceTimeKey, handle timestamp metadata

    // ToDo: configure local paths
    val projectdir = "data/workflowexample/"

    val inputdir = projectdir + "in/"
    val outputdir = projectdir + "out/"

    val input_label = inputdir + "labels.tif"
    //val input_dop = inputdir + "dop.tif"
    val input_sat = inputdir + "S2_2016-05-08.tif"
    //val input_sat = inputdir + "S2_2016-07-18.tif"
    //val input_sat = inputdir + "S2_2016-09-15.tif"

    val output_result = outputdir + "result/result.tif"

    val output_labeled_layerstack =  outputdir + "layerstack/labeled_layerstack.tif"

    val useLayerstackExport = false
    val useResultExport = true
    val useCleanup = true
    val useWebMercator = true
    val useLeaflet = false

    val (layer_label, layer_sat) =
      ("layer_label", "layer_sat")
    if(useWebMercator){
      MultibandGeotiffTilingExample(input_label, layer_label)
      MultibandGeotiffTilingExample(input_sat, layer_sat)
    } else { //Debugging (w/o WebMercator, uses original crs)
      GeotiffToMultibandLayer(input_label, layer_label)
      GeotiffToMultibandLayer(input_sat, layer_sat)
    }

    val labeled_layerstack = {
        val layer_label_sat = "layer_label_sat"
        ManyLayersToMultibandLayer(layer_label,  layer_sat,  layer_label_sat)
        layer_label_sat
      }

    if(useCleanup){
      DeleteLayer(layer_label)
      DeleteLayer(layer_sat)
    }

    if(useLayerstackExport){
      MultibandLayerToGeotiff(labeled_layerstack, output_labeled_layerstack)
    }

    val layer_result = "layer_result"
    TilePixelingExample(labeled_layerstack, layer_result)
    // ToDo: Send Pixel Stream to Kafka
    // ToDo: Receive Result from Kafka
    // ToDo: store Result RDD als Hadoop Layer layer_result

    // Export Result to GeoTiff
    if(useResultExport){
      LayerToGeotiff(layer_result, output_result)
    }

    // Visualize Result
    if(useLeaflet && useWebMercator) {
      LayerToPyramid(catalogPath, layer_result)
      ServeLayerAsMap(catalogPath, layer_result)
    }

  }

}
