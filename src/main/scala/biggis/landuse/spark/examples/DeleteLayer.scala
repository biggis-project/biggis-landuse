package biggis.landuse.spark.examples

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{SparkContext, SparkException}

object DeleteLayer extends App with LazyLogging {
  try {
    //val Array(catalogPath, layerName) = args
    val Array(layerName, catalogPath) = args
    implicit val sc = Utils.initSparkAutoContext
    DeleteLayer(layerName)(catalogPath, sc)
    sc.stop()
  } catch {
    //case _: MatchError => println("Run as: /path/to/catalog layerName")
    case _: MatchError => println("Run as: layerName /path/to/catalog")
    case e: SparkException => logger error e.getMessage + ". Try to set JVM parameter: -Dspark.master=local[*]"
  }

  def apply(layerName: String)(implicit catalogPath: String, sc: SparkContext): Unit = {
    logger info s"Deleting layer $layerName including all zoom levels in catalog $catalogPath ..."

    biggis.landuse.api.deleteLayerFromCatalog(layerName)(catalogPath, sc)

    logger info "done"
  }

}