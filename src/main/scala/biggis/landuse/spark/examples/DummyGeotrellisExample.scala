package biggis.landuse.spark.examples

import geotrellis.spark.LayerId
import geotrellis.spark.io.hadoop.HadoopLayerReader
import geotrellis.spark.io.hadoop.HadoopLayerWriter
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object DummyGeotrellisExample extends App {

  val catalogPath = "testCatalog" // CONFIG
  val layerName = "testLayer" // INPUT

  //    implicit val sc = Utils.initSparkAutoContext

  val sparkConf = new SparkConf()
  sparkConf.setAppName("Geotrellis Example")
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sparkConf.set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
  sparkConf.setMaster("local[*]")
  implicit val sc = new SparkContext(sparkConf)

  val layerId = LayerId(layerName, 0)

  val writer = HadoopLayerWriter(new Path(catalogPath))

  // write some metadata to layer
  import spray.json.DefaultJsonProtocol._
  val attributeStore = writer.attributeStore
  attributeStore.write(layerId, "metadata", "some content")

  // check that we created the layer
  val isThere = writer.attributeStore.layerExists(layerId) // OUTPUT


  sc.stop()

  if (isThere)
    println("the layer is there")
  else
    println("not there")
}
