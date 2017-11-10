package biggis.landuse.spark.examples

import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.{DoubleConstantNoDataCellType, NODATA, Tile, isData}
import geotrellis.raster.render.ColorMap
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerReader, HadoopLayerWriter}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod.spatialKeyIndexMethod
import geotrellis.spark.io.{SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.rdd.RDD

object NDVILayerExample extends LazyLogging {

  // from geotrellis/geotrellis-landsat-tutorial -> src/main/resources/application.conf
  // tutorial.ndviColormap = "0:ffffe5ff;0.1:f7fcb9ff;0.2:d9f0a3ff;0.3:addd8eff;0.4:78c679ff;0.5:41ab5dff;0.6:238443ff;0.7:006837ff;1:004529ff"
  var colorMap : ColorMap = ColorMap.fromStringDouble(s"0:ffffe5ff;0.1:f7fcb9ff;0.2:d9f0a3ff;0.3:addd8eff;0.4:78c679ff;0.5:41ab5dff;0.6:238443ff;0.7:006837ff;1:004529ff").get

  def main(args: Array[String]): Unit = {
    try {
      val Array(layerNIR, layerRed, layerNDVI, catalogPath) = args
      implicit val sc = Utils.initSparkAutoContext
      NDVILayerExample(layerNIR, layerRed, layerNDVI)(catalogPath, sc)
      sc.stop()
    } catch {
      case _: MatchError => println("Run as: layerNIR layerRed layerNDVI /path/to/catalog")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
    }
  }

  def apply(layerNIR: String, layerRed: String, layerNDVI: String)(implicit catalogPath: String, sc: SparkContext): Unit = {
    logger info s"Running ndvi calc of layers '$layerNIR' - '$layerRed' in catalog '$catalogPath'"

    // Create the attributes store that will tell us information about our catalog.
    val catalogPathHdfs = new Path(catalogPath)
    implicit val attributeStore = HadoopAttributeStore( catalogPathHdfs )
    val layerReader = HadoopLayerReader(attributeStore)

    // see: geotrellis-landsat-tutorial/src/main/scala/tutorial/IngestImage.scala
    // https://github.com/geotrellis/geotrellis-landsat-tutorial/blob/master/src/main/scala/tutorial/IngestImage.scala
    // replaced by GeotiffTilingExample -> Hadoop Layer
    val commonZoom = Math.max(findFinestZoom(layerNIR), findFinestZoom(layerRed))
    val layerIdNIR = findLayerIdByNameAndZoom(layerNIR, commonZoom)
    val layerIdRed = findLayerIdByNameAndZoom(layerRed, commonZoom)
    println(s"$layerIdNIR, $layerIdRed")
    val tilesNIR: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      layerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerIdNIR)
    val tilesRed: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      layerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerIdRed)

    // see:  geotrellis-landsat-tutorial/src/main/scala/tutorial/Calculations.scala
    // https://github.com/geotrellis/geotrellis-landsat-tutorial/blob/master/src/main/scala/tutorial/Calculations.scala
    def ndvi (r: Double, ir: Double) : Double = {
      if (isData(r) && isData(ir)) {
        (ir - r) / (ir + r)
      } else {
        Double.NaN
      }
    }

    // here, the calculation takes place
    val ndviRdd: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      tilesNIR.withContext { rdd => rdd
        .join(tilesRed)
        .map { case (spatialKey, (tileNIR, tileRed)) =>
          val tileNIRFloat = tileNIR.convert(DoubleConstantNoDataCellType)
          val tileRedFloat = tileRed.convert(DoubleConstantNoDataCellType)
          val tile = tileRedFloat.combineDouble(tileNIRFloat) {
            (r: Double, ir: Double) => ndvi(r,ir)
          }
          (spatialKey, tile)
        }
      }

    // this will be the new ndvi layer
    val layerIdNDVI = LayerId(layerNDVI, commonZoom)

    // automatically deleting existing layer
    if (attributeStore.layerExists(layerIdNDVI)) {
      logger debug s"Layer $layerIdNDVI already exists, deleting ..."
      HadoopLayerDeleter(attributeStore).delete(layerIdNDVI)
    }

    logger info s"Writing convoluted layer '${layerIdNDVI}'"
    val writer =  HadoopLayerWriter(catalogPathHdfs, attributeStore)
    writer.write(layerIdNDVI, ndviRdd, ZCurveKeyIndexMethod)

    Utils.writeHistogram(attributeStore, layerNDVI, ndviRdd.histogram)

    logger info "done."
  }

  def findFinestZoom(layerName: String)(implicit attributeStore: HadoopAttributeStore): Int = {
    val zoomsOfLayer = attributeStore.layerIds filter (_.name == layerName)
    zoomsOfLayer.sortBy(_.zoom).last.zoom
  }

  def findLayerIdByNameAndZoom(layerName: String, zoom: Int)(implicit attributeStore: HadoopAttributeStore): LayerId = {
    val zoomsOfLayer = attributeStore.layerIds filter (_.name == layerName)
    zoomsOfLayer.filter(_.zoom == zoom).head
  }
}
