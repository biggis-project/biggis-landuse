package biggis.landuse.spark.examples

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkException

object DeleteLayer extends App with LazyLogging {
  try {

    val Array(layerName, catalogPath) = args
    val sc = Utils.initSparkContext

    logger info s"Deleting layer $layerName including all zoom levels in catalog $catalogPath ..."

    biggis.landuse.api.deleteLayerFromCatalog(layerName)(catalogPath, sc)
    sc.stop()

    logger info "done"

  } catch {
    case _: MatchError => println("Run as: layerName /path/to/catalog")
    case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
  }
}