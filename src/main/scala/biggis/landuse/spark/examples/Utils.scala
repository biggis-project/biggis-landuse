package biggis.landuse.spark.examples

import java.lang.management.ManagementFactory

import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.io.HistogramDoubleFormat
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.LayerId
import geotrellis.spark.io.AttributeStore
import org.apache.spark.{SparkConf, SparkContext, SparkException}

import scala.collection.JavaConverters._

/**
  * Created by Viliam Simko on 2016-11-04
  */
object Utils extends LazyLogging {

  val TILE_SIZE = 256
  val RDD_PARTITIONS = 32
  val RESAMPLING_METHOD = Bilinear

  def initSparkAutoContext: SparkContext = {
    logger info s"initSparkAutoContext "
    val args:List[String] = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala.toList

    return args.find(_ == "-Dspark.master=local[*]") match {
      case Some(_) => {
        logger info s"calling initSparkContext"
        initSparkContext
      }
      case None => initSparkClusterContext
    }
  }

  @deprecated("do not use, only for dirty debugging")
  def initLocalSparkContext: SparkContext = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Geotrellis Example")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
    sparkConf.setMaster("local[*]")

    return new SparkContext(sparkConf)
  }

  def initSparkContext: SparkContext = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Geotrellis Example")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    // We also need to set the spark master.
    // instead of  hardcoding it using sparkConf.setMaster("local[*]")
    // we can use the JVM parameter: -Dspark.master=local[*]
    // sparkConf.setMaster("local[*]")

    return new SparkContext(sparkConf)
  }

  def initSparkClusterContext: SparkContext = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //TODO: get rid of the hardcoded JAR
    sparkConf.setJars(Seq("hdfs:///jobs/landuse-example/biggis-landuse-0.0.4-SNAPSHOT.jar"))

    // We also need to set the spark master.
    // instead of  hardcoding it using sparkConf.setMaster("local[*]")
    // we can use the JVM parameter: -Dspark.master=local[*]
    // sparkConf.setMaster("local[*]")

    return new SparkContext(sparkConf)
  }

  def writeHistogram(attributeStore: AttributeStore, layerName: String, histogram: Histogram[Double]): Unit = {
    logger debug s"Writing histogram of layer '$layerName' to attribute store as 'histogramData' for zoom level 0"
    attributeStore.write(
      LayerId(layerName, 0), "histogramData", histogram)
  }

}
