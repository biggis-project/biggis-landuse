package biggis.landuse.spark.examples

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.{SparkContext, SparkException}

/**
  * Created by ak on 03.04.2017.
  *
  * see: https://github.com/geotrellis/geotrellis-landsat-tutorial
  *
  * biggis-landuse/
  * data/geotrellis-landsat-tutorial
  */
object GettingStarted extends LazyLogging {
    def main(args: Array[String]): Unit = {
      try {
        val Array(catalogPath) = args
        //implicit val catalogPath = "target/geotrellis-catalog/"
        implicit val sc = Utils.initSparkContext
        GettingStarted()(catalogPath, sc)
        sc.stop()
      }
      catch {
        case _: MatchError => println("Run as: /path/to/catalog")
        case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
      }
    }

    def apply()(implicit catalogPath: String, sc: SparkContext): Unit = {
      /**
        * see: https://github.com/geotrellis/geotrellis-landsat-tutorial
        *
        * download data, see: data/geotrellis-landsat-tutorial/readme.txt
        *
        * see: https://en.wikipedia.org/wiki/Landsat_8
        * Band 3 - Green 	0.525 – 0.600 µm 	30 m 	1826 W/(m²µm)
        * Band 4 - Red 	0.630 – 0.680 µm 	30 m 	1574 W/(m²µm)
        * Band 5 - Near Infrared 	0.845 – 0.885 µm 	30 m 	955 W/(m²µm)
        *
        * To Debug in IDE, please set VM options to
        *   -Dspark.master=local[*]
        * and program arguments to
        *   target/geotrellis-catalog
        */
      val path = "data/geotrellis-landsat-tutorial/"

      def bandPath(b: String) = s"LC81070352015218LGN00_${b}.TIF"
      val (layer_L8_B3_green, layer_L8_B4_red, layer_L8_B5_nir, layer_L8_BQA_clouds) =
        ("layer_L8_B3_green", "layer_L8_B4_red", "layer_L8_B5_nir" ,"layer_L8_BQA_clouds")
      val (file_L8_B3_green, file_L8_B4_red, file_L8_B5_nir, file_L8_BQA_clouds) =
        ( bandPath("B3"), bandPath("B4"), bandPath("B5"), bandPath("BQA"))

      // see: geotrellis-landsat-tutorial/src/main/scala/tutorial/IngestImage.scala
      // https://github.com/geotrellis/geotrellis-landsat-tutorial/blob/master/src/main/scala/tutorial/IngestImage.scala
      // replaced by GeotiffTilingExample
      GeotiffTilingExample( path + file_L8_B3_green, layer_L8_B3_green)
      GeotiffTilingExample( path + file_L8_B4_red, layer_L8_B4_red)
      GeotiffTilingExample( path + file_L8_B5_nir, layer_L8_B5_nir)
      GeotiffTilingExample( path + file_L8_BQA_clouds, layer_L8_BQA_clouds)

      val layer_NDVI = "layer_NDVI"
      //NDVILayerExample( layer_L8_B5_nir, layer_L8_B4_red, layer_NDVI)
      NDVILayerWithCloudMaskExample( layer_L8_B5_nir, layer_L8_B4_red, layer_L8_BQA_clouds, layer_NDVI)

      // Save NDVI To File
      //LayerToGeotiff(layer_NDVI, path + "test/" + bandPath("NDVI"))   // ToDo: fix stitching, file too big

      // Serve Layer via Leaflet static/GettingStarted.html
      LayerToPyramid(catalogPath, layer_NDVI)
      ServeLayerAsMap(catalogPath, layer_NDVI)
    }
}
