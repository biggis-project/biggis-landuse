package biggis.landuse.spark.examples

import geotrellis.proj4.WebMercator
import geotrellis.raster.Tile
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer.Options
import geotrellis.spark.Bounds
import geotrellis.spark.LayerId
import geotrellis.spark.Metadata
import geotrellis.spark.SpatialKey
import geotrellis.spark.TileLayerMetadata
import geotrellis.spark.TileLayerRDD
import geotrellis.spark.io.hadoop.HadoopAttributeStore
import geotrellis.spark.io.hadoop.HadoopLayerWriter
import geotrellis.spark.rasterize.RasterizeFeaturesRDD
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.util.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD

/**
  * Created by ak on 21.06.2017.
  */
object ShapefilePolygonRasterizer extends LazyLogging {
  def main(args: Array[String]): Unit = {
    try {
      val Array(shapefilePath, attribName, layerName, catalogPath) = args
      implicit val sc: SparkContext = Utils.initSparkContext
      ShapefilePolygonRasterizer(shapefilePath, attribName, layerName)(catalogPath, sc)
      sc.stop()
    } catch {
      case _: MatchError => println("Run as: shapefilePath attribName layerName /path/to/catalog")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
    }
  }

  def apply(shapefileName: String, attribName: String, layerName: String)(implicit catalogPath: String, sc: SparkContext): Unit = {
    logger info s"Running rasterizer for layer '$layerName' in catalog '$catalogPath'"

    val multipolygons = UtilsShape.readShapefileMultiPolygonDoubleAttribute(shapefileName, attribName)
    val multipolygons_extent = UtilsShape.getExtent(multipolygons)

    // Create the attributes store that will tell us information about our catalog.
    val catalogPathHdfs = new Path(catalogPath)
    val attributeStore = HadoopAttributeStore(catalogPathHdfs)

    // Create the writer that we will use to store the tiles in the local catalog.
    val layerWriter = HadoopLayerWriter(catalogPathHdfs, attributeStore)

    val cellSize = CellSize(100, 100) // TODO cell size should be a parameter
    val myLayout = LayoutDefinition(GridExtent(multipolygons_extent, cellSize), Utils.TILE_SIZE, Utils.TILE_SIZE)
    val cellType = IntConstantNoDataCellType
    val crs = WebMercator
    val bounds = Bounds[SpatialKey](
      SpatialKey(0, 0),
      SpatialKey(myLayout.layoutCols, myLayout.layoutRows)
    )

    val myTileLayerMetadata = TileLayerMetadata(cellType, myLayout,
      multipolygons_extent,
      crs,
      bounds)

    val zoom = 15 // TODO how to automatically/manually select the right zoom level
    //    val srcLayerId = zoomsOfLayer.sortBy(_.zoom).last

    val multipolygonsRdd = sc.parallelize(multipolygons)
    val rasterizedRDD: RDD[(SpatialKey, Tile)] with Metadata[LayoutDefinition] = RasterizeFeaturesRDD
      .fromFeature(
        multipolygonsRdd,
        IntConstantNoDataCellType,
        myLayout,
        Options.DEFAULT)

    val rasterizedRDDwithContext: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      TileLayerRDD(rasterizedRDD, myTileLayerMetadata)

    val dstLayerId = LayerId(layerName, zoom)
    logger debug s"The following layerId will be used for writing rastertized shapefile: $dstLayerId"

    biggis.landuse.api.deleteLayerFromCatalog(dstLayerId)
    biggis.landuse.api.writeRddToLayer(rasterizedRDDwithContext, dstLayerId)

    logger info "shapefile rasterization finished"
  }
}
