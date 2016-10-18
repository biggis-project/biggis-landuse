package biggis.landuse.spark.examples

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.{SparkConf, SparkContext}
import geotrellis.shapefile.ShapeFileReader
import geotrellis.vector._
import geotrellis.raster._
import scala.collection.JavaConverters._
import com.vividsolutions.jts.{geom => jts}

/*
 * Needs additional dependencies from external repositories
 * see: http://stackoverflow.com/questions/16225573/why-cant-i-resolve-the-dependencies-for-geotools-maven-quickstart
 */

object ShapefileExample extends LazyLogging {
  def main(args: Array[String]): Unit = {
    try {
      val Array(shapeName, catalogPath) = args
      ShapefileExample(shapeName)(catalogPath)
    } catch {
      case _: MatchError => println("Run as: shapeName /path/to/catalog")
    }
  }

  def apply(shapeName: String)(implicit catalogPath: String): Unit = {
    logger info s"Running Shapefile import '$shapeName' in catalog '$catalogPath'"

    val sparkConf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Geotrellis-based convolution of a layer using circular kernel")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    implicit val sc = new SparkContext(sparkConf)

    val shp = ShapeFileReader.readSimpleFeatures(shapeName)
    for(ft <- shp) yield{
      val ID : String = ft.getID
      val geom = ft.getDefaultGeometry
      val attribs = ft.getAttributes
      attribs.remove(0)
      println(ID)
      println(geom)
      println(attribs)
    }
    //ToDo: Get Shapefile Context (JTS Geometry Format) into Spark Context

    //https://gitter.im/geotrellis/geotrellis/archives/2015/05/18
    //ShapeFile.readPointFeatures[Int](path, dataField)
    //geotrellis.raster.VectorToRaster(pointsFromShapeFile, kernel, rasterExtent)
    /*

    // https://gist.github.com/echeipesh/26b50b235fd812f39098
    val mps: Seq[MultiPolygon[Int]] =
      for (ft <- shp) yield {
        val geom = ft.getAttribute(0).asInstanceOf[jts.MultiPolygon]
        val props: Map[String, Object] =
          ft.getProperties.asScala.drop(1).map { p =>
            (p.getName.toString, ft.getAttribute(p.getName))
          }.toMap
        val data = props("WorkingAge").asInstanceOf[Long].toInt
        new MultiPolygon(geom, data)
      }
    */

    /*
    val extent = {
      val env = mps
        .map(_.geom.getEnvelope())
        .reduce(_ union _)
        .getEnvelopeInternal()
        .asInstanceOf[jts.Envelope]

      Extent(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
    }
    val re = RasterExtent(extent, 255, 255)
    val rd = RasterData.emptyByType(TypeInt, 255, 255).mutable
    println(mps)
    for { mp <- mps; poly <- mp.flatten } {
      Rasterizer.foreachCellByFeature[Polygon, Int](poly, re) {
        new Callback[Polygon, Int] {
          def apply(col: Int, row: Int, g: Polygon[Int]) =
            rd.set(col, row, g.data)
        }
      }
    }

    val rater = Raster(rd, re)
    */
  }
}
