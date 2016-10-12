package biggis.landuse.spark.examples

import com.typesafe.scalalogging.slf4j.LazyLogging
import geotrellis.spark.io.file.{FileAttributeStore, FileLayerDeleter}

/**
  * Created by Viliam Simko on 10/4/16.
  */
object DeleteLayer extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val Array(catalogPath, layerName) = args
    DeleteLayer(layerName)(catalogPath)
  }

  def apply(layerName: String)(implicit catalogPath: String): Unit = {
    logger info s"Deleting layer $layerName including all zoom levels in catalog $catalogPath ..."

    val attributeStore = FileAttributeStore(catalogPath)
    val deleter = FileLayerDeleter(attributeStore)
    attributeStore.layerIds.filter(_.name == layerName).foreach(deleter.delete)

    logger info "done"
  }

}