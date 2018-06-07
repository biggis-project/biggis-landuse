package biggis.landuse.spark.examples
//package geotrellis.spark.etl
import geotrellis.spark.etl

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import geotrellis.raster.{Tile,MultibandTile}
import geotrellis.spark._
import geotrellis.spark.etl.Etl
import geotrellis.spark.etl.config.EtlConf
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.ProjectedExtent

object GeoTrellisETL extends StrictLogging {
  type I = ProjectedExtent // or TemporalProjectedExtent for temporal ingest
  type K = SpatialKey // or SpaceTimeKey for temporal ingest
  type V = MultibandTile //Tile // or MultibandTile to ingest multiband tile
  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = biggis.landuse.api.initSparkSession
    implicit val sc : SparkContext = sparkSession.sparkContext //SparkUtils.createSparkContext("GeoTrellis ETL", new SparkConf(true))
    //implicit val sc = biggis.landuse.spark.examples.Utils.initSparkAutoContext// SparkUtils.createSparkContext("GeoTrellis ETL", new SparkConf(true))
    try {
      //GeoTrellisETL(args)(sc/*, sparkSession*/)
      SinglebandIngest(args)
      //MultibandIngest(args)
    } finally {
      sc.stop()
    }
  }

  def apply(args: Array[String])(implicit sc: SparkContext/*, sparkSession: SparkSession*/) = {
    EtlConf(args) foreach { conf =>
      /* parse command line arguments */
      val etl = Etl(conf)
      /* load source tiles using input module specified */
      val sourceTiles = etl.load[I, V]  //etl.load[ProjectedExtent, MultibandTile]
      /* perform the reprojection and mosaicing step to fit tiles to LayoutScheme specified */
      val (zoom, tiled) = etl.tile[I, V, K](sourceTiles)  //etl.tile[ProjectedExtent, MultibandTile, SpatialKey](sourceTiles)
      /* save and optionally pyramid the mosaiced layer */
      etl.save[K, V](LayerId(etl.input.name, zoom), tiled)  //etl.save[SpatialKey, MultibandTile](LayerId(etl.input.name, zoom), tiled)
    }
  }

  def SinglebandIngest(args: Array[String])(implicit sc: SparkContext/*, sparkSession: SparkSession*/): Unit = {
    //implicit val sc = SparkUtils.createSparkContext("GeoTrellis ETL SinglebandIngest", new SparkConf(true))
    try {
      Etl.ingest[ProjectedExtent, SpatialKey, Tile](args)
    } finally {
      sc.stop()
    }
  }

  def MultibandIngest(args: Array[String])(implicit sc: SparkContext/*, sparkSession: SparkSession*/): Unit = {
    //implicit val sc = SparkUtils.createSparkContext("GeoTrellis ETL MultibandIngest", new SparkConf(true))
    try {
      Etl.ingest[ProjectedExtent, SpatialKey, Tile](args)
    } finally {
      sc.stop()
    }
  }
}