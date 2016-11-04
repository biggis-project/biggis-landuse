package biggis.landuse.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Viliam Simko on 2016-11-04
  */
object Utils {

  def initSparkContext(): SparkContext = {
    val sparkConf = new SparkConf()
      .setAppName("Geotrellis Example")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    // We also need to set the spark master.
    // instead of  hardcoding it using spakrConf.setMaster("local[*]")
    // we can use the JVM parameter: -Dspark.master=local[*]
    // sparkConf.setMaster("local[*]")

    return new SparkContext(sparkConf)
  }
}
