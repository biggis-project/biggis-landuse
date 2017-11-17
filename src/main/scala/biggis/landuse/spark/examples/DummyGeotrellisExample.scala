package biggis.landuse.spark.examples

import com.typesafe.scalalogging.LazyLogging
import geotrellis.spark.LayerId
import geotrellis.spark.io.hadoop.HadoopLayerWriter
import org.apache.hadoop.fs.Path


object DummyGeotrellisExample extends LazyLogging {

  def main(args: Array[String]): Unit = {

    val catalogPath = "testCatalog"
    val layerName = "testLayer"

    implicit val sc = Utils.initSparkAutoContext

    val writer = HadoopLayerWriter(new Path(catalogPath))
    val isThere = writer.attributeStore.layerExists(LayerId(layerName, 10))

    sc.stop()

    logger debug s"Does the layer $layerName in catalog $catalogPath exist ? $isThere"
  }

}
