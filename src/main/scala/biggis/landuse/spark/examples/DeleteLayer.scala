package biggis.landuse.spark.examples

import com.typesafe.scalalogging.slf4j.LazyLogging
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Viliam Simko on 10/4/16.
  */
object DeleteLayer extends LazyLogging {

  def main(args: Array[String]): Unit = {
    try {
      val Array(catalogPath, layerName) = args
      DeleteLayer(layerName)(catalogPath)
    } catch {
      case _: MatchError => println("Run as: /path/to/catalog layerName")
    }
  }

  def apply(layerName: String)(implicit catalogPath: String): Unit = {
    logger info s"Deleting layer $layerName including all zoom levels in catalog $catalogPath ..."

    val sparkConf =
      new SparkConf()
        .setAppName("Spark Tiler")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    // We also need to set the spark master.
    // instead of  hardcoding it using spakrConf.setMaster("local[*]")
    // we can use the JVM parameter: -Dspark.master=local[*]
    // sparkConf.setMaster("local[*]")

    implicit val sc = new SparkContext(sparkConf)

    val hdfsPath = new Path(catalogPath)
    val attributeStore = HadoopAttributeStore(hdfsPath)

    val deleter = HadoopLayerDeleter(attributeStore)
    attributeStore.layerIds.filter(_.name == layerName).foreach(deleter.delete)

    logger info "done"
  }

}