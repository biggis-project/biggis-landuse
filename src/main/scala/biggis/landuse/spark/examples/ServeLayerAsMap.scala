package biggis.landuse.spark.examples

import akka.actor._
import akka.io.IO
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster._
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.io.HistogramDoubleFormat
import geotrellis.raster.render._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop.HadoopValueReader
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import spray.can.Http
import spray.http.MediaTypes
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.routing.Directive.pimpApply
import spray.routing.HttpService

import scala.concurrent._

object ServeLayerAsMap extends LazyLogging {

  // filled from command line
  var fileValueReader: HadoopValueReader = null
  var layerNameServed: String = null
  var colorMap: ColorMap = null
  // end of filled from command line

  // the reader is used from the akka actor class
  def reader(layerId: LayerId) = fileValueReader.reader[SpatialKey, Tile](layerId)

  // DEBUG: val args = Array("target/geotrellis-catalog", "morning2")
  def main(args: Array[String]): Unit = {
    try {
      val Array(catalogPath, layerName) = args

      logger info "setting variables from commandline"

      //layerNameServed = layerName // TODO // moved to apply(...) using init()

      implicit val sc = Utils.initSparkAutoContext

      // catalog reader // moved to apply(...) using init()
      //fileValueReader = HadoopValueReader(new Path(catalogPath))

      // read quantile breaks from attribute store  // moved to apply(...) using initHeatMap(...)
      //val layerId = LayerId(layerName, 0)
      //val hist = fileValueReader.attributeStore.read[Histogram[Double]](layerId, "histogramData")
      //colorMap = ColorRamps.HeatmapBlueToYellowToRedSpectrum.toColorMap(hist.quantileBreaks(10))

      ServeLayerAsMap(catalogPath, layerName)
    } catch {
      case _: MatchError => println("Run as: [/path/to/catalog] [layerName]")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
    }
  }

  def apply(catalogPath: String, layerNameServed: String, colorMap: ColorMap = null)(implicit sc: SparkContext): Unit = {
    // init catalog reader
    init(catalogPath, layerNameServed)
    // init ColorMap
    ServeLayerAsMap.colorMap = if (colorMap == null) initHeatmap(layerNameServed) else colorMap

    logger info s"Serving layer='$layerNameServed' from catalog='$catalogPath'"

    implicit val system = akka.actor.ActorSystem("biggis-actor-system")

    // create and start our service actor
    val service = system.actorOf(Props(classOf[ServeLayerAsMapActor]), "tile-server")

    // start a new HTTP server on port 8080 with our service actor as the handler
    IO(Http) ! Http.Bind(service, "localhost", 18080)
    println("Now open the file 'static/index.html' in your browser.")
    println("The HTML code uses leaflet javascript library which communicates with our tile-serving backend.")
  }

  // init catalog reader
  def init(catalogPath: String, layerNameServed: String)(implicit sc: SparkContext): Unit = {
    ServeLayerAsMap.layerNameServed = layerNameServed
    // catalog reader
    ServeLayerAsMap.fileValueReader = HadoopValueReader(new Path(catalogPath))
  }

  // init ColorMap
  def initHeatmap(layerNameServed: String): ColorMap = {
    // read quantile breaks from attribute store
    val layerId = LayerId(layerNameServed, 0)
    val hist = fileValueReader.attributeStore.read[Histogram[Double]](layerId, "histogramData")
    colorMap = ColorRamps.HeatmapBlueToYellowToRedSpectrum.toColorMap(hist.quantileBreaks(10))
    colorMap
  }
}

class ServeLayerAsMapActor extends Actor with HttpService {

  import scala.concurrent.ExecutionContext.Implicits.global

  def actorRefFactory = context

  def receive = runRoute(root)

  def root =
    pathPrefix(IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
      respondWithMediaType(MediaTypes.`image/png`) {
        complete {
          Future {
            try {
              val tile: Tile = ServeLayerAsMap.reader(LayerId(ServeLayerAsMap.layerNameServed, zoom)).read(x, y)
              val png = tile.renderPng(ServeLayerAsMap.colorMap)
              Some(png.bytes)
            } catch {
              //// https://github.com/locationtech/geotrellis/commit/69ee528d99e4d126bd7dbf464ce7805fe4fe33d9
              case _: ValueNotFoundError => None // TileNotFoundError in Geotrellis 0.10.3
              //case _: TileNotFoundError => None
              case _: UnsupportedOperationException => None
            }
          }
        }
      }
    }
}
