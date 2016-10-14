package biggis.landuse.spark.examples

import com.typesafe.scalalogging.slf4j.StrictLogging
import geotrellis.raster.Tile
import geotrellis.spark._
import geotrellis.spark.etl.Etl
import geotrellis.spark.io.index._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.etl.EtlConf
import geotrellis.raster.MultibandTile
import geotrellis.raster.io.geotiff._
import geotrellis.proj4._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent,ProjectedExtent}
//import geotrellis.config.json.dataset.JConfig
import org.apache.spark.SparkConf
//import geotrellis.gdal._

//https://mvnrepository.com/artifact/com.azavea.geotrellis

/*
object SinglebandIngest {
  def main(args: Array[String]): Unit = {
    implicit val sc = SparkUtils.createSparkContext("GeoTrellis ETL SinglebandIngest", new SparkConf(true))
    try {
      Etl.ingest[ProjectedExtent, SpatialKey, Tile](args)
    } finally {
      sc.stop()
    }
  }
}
*/
/*
object TestGdalReader {
  val firstBand: (Tile, RasterExtent) =
    GdalReader.read(path = "/path/to/my/file.he5", band = 1)
}
*/

object GeoTrellisETL extends StrictLogging {
  def main(args: Array[String]): Unit = {
    //val Array(input, output) = args
    GeoTrellisETL(args)
  }

  def apply(args: Array[String])(): Unit = {
    logger info s"GeoTrellis ETL MultibandIngest"
    //GeoTrellisETL
    implicit val sc = SparkUtils.createSparkContext("GeoTrellis ETL MultibandIngest",
      new SparkConf(true)
        .setMaster("local[*]")
        .setAppName("GeoTrellis ETL MultibandIngest")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
    )
    try {
      Etl.ingest[ProjectedExtent, SpatialKey, MultibandTile](args,geotrellis.spark.io.index.ZCurveKeyIndexMethod)
      //geotrellis.spark.etl.MultibandIngest.main(args)
      //Etl.ingest[ProjectedExtent, SpatialKey, MultibandTile](args)
    } finally {
      sc.stop()
    }
    /*
    type I = ProjectedExtent // or TemporalProjectedExtent for temporal ingest
    type K = SpatialKey // or SpaceTimeKey for temporal ingest
    type V = Tile // or MultibandTile to ingest multiband tile

    //val modules = new Etl.defaultModules
    //val conf : EtlConf  = new EtlConf(args)
    try {
      EtlConf(args) foreach { conf  =>
        /* parse command line arguments */
        val etl = Etl(conf, modules)
        /* load source tiles using input module specified */
        val sourceTiles = etl.load[I, V]
        /* perform the reprojection and mosaicing step to fit tiles to LayoutScheme specified */
        val (zoom, tiled) = etl.tile(sourceTiles)
        /* save and optionally pyramid the mosaiced layer */
        etl.save[K, V](LayerId(etl.input.name, zoom), tiled)
      }
    } finally {
      sc.stop()
    }
    */
  }
}

