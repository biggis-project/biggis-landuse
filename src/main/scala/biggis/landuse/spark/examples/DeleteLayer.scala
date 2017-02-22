package biggis.landuse.spark.examples

import com.typesafe.scalalogging.LazyLogging
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext, SparkException}

object DeleteLayer extends App with LazyLogging {

  try {
    val Array(catalogPath, layerName) = args
    implicit val sc = Utils.initSparkContext
    DeleteLayer(layerName)(catalogPath, sc)
    sc.stop()
  } catch {
    case _: MatchError => println("Run as: /path/to/catalog layerName")
    case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
  }

  def apply(layerName: String)(implicit catalogPath: String, sc: SparkContext): Unit = {
    logger info s"Deleting layer $layerName including all zoom levels in catalog $catalogPath ..."

    //implicit val sc = Utils.initSparkContext

    val hdfsPath = new Path(catalogPath)
    val attributeStore = HadoopAttributeStore(hdfsPath)

    val deleter = HadoopLayerDeleter(attributeStore)
    attributeStore.layerIds filter (_.name == layerName) foreach { deleter.delete }
    // TODO: also remove the empty directory "layerName"

    logger info "done"
  }

}