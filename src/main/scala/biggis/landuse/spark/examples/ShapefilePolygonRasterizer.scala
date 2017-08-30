package biggis.landuse.spark.examples

import geotrellis.util.LazyLogging
import geotrellis.shapefile.ShapeFileReader
import geotrellis.vector._
import geotrellis.raster._
import org.apache.spark.{SparkContext, SparkException}
import geotrellis.raster.rasterize
import geotrellis.raster.io.HistogramDoubleFormat
import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster.{Tile, withTileMethods}
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerReader, HadoopLayerWriter}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod.spatialKeyIndexMethod
import geotrellis.spark.io.{SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.spark.{LayerId, Metadata, MultibandTileLayerRDD, SpatialKey, TileLayerMetadata, TileLayerRDD}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import com.vividsolutions.jts.{geom => jts}
import geotrellis.proj4.WebMercator
import geotrellis.spark.rasterize.RasterizeRDD
import geotrellis.spark.tiling.{FloatingLayoutScheme, LayoutDefinition, ZoomedLayoutScheme}
import geotrellis.raster.rasterize.Rasterizer.Options
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerWriter, HadoopSparkContextMethodsWrapper}
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.rasterize.RasterizeFeaturesRDD
import org.osgeo.proj4j.CoordinateReferenceSystem

/**
  * Created by ak on 21.06.2017.
  */
object ShapefilePolygonRasterizer extends LazyLogging {
  def main(args: Array[String]): Unit = {
    try {
      val Array(shapefilePath, attribName, layerName, catalogPath) = args
      implicit val sc = Utils.initSparkContext
      ShapefilePolygonRasterizer(shapefilePath, attribName, layerName)(catalogPath, sc)
      sc.stop()
    } catch {
      case _: MatchError => println("Run as: shapefilePath attribName layerName /path/to/catalog")
      case e: SparkException => logger error e.getMessage + ". Try to set JVM parmaeter: -Dspark.master=local[*]"
    }
  }

  def apply(shapefileName: String, attribName: String, layerName: String)(implicit catalogPath: String, sc: SparkContext): Unit = {
    logger info s"Running rasterizer for layer '$layerName' in catalog '$catalogPath'"

    val multipolygons = UtilsShape.readShapefileMultiPolygonDoubleAttribute(shapefileName,attribName)
    val multipolygons_extent = UtilsShape.getExtent(multipolygons)

    // Create the attributes store that will tell us information about our catalog.
    val catalogPathHdfs = new Path(catalogPath)
    val attributeStore = HadoopAttributeStore( catalogPathHdfs )

    // Create the writer that we will use to store the tiles in the local catalog.
    val layerWriter =  HadoopLayerWriter(catalogPathHdfs, attributeStore)
    val layerReader =  HadoopLayerReader(catalogPathHdfs)

    val (polys, values) = (multipolygons.map( item => item.geom ), multipolygons.map( item => item.data ))
    //val (polys, values) = (multipolygons.map( item => item._1.asInstanceOf[MultiPolygon] ), multipolygons.map( item => item._2 ))

    // ToDo: compare with "real" raster data (and get Metadata)
    val zoomsOfLayer = attributeStore.layerIds filter (_.name == "layer_label")
    //val zoomsOfLayer = attributeStore.layerIds filter (_.name == layerName)
    if(zoomsOfLayer.isEmpty) {
      logger info s"Layer '$layerName' not found in the catalog '$catalogPath'"
      return
    }
    val srcLayerId = zoomsOfLayer.sortBy(_.zoom).last
    logger debug s"The following layerId will be used: $srcLayerId"

    val inputRdd:RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = layerReader
      .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](srcLayerId)

    /*
    import geotrellis.vector._
    import geotrellis.vector.io._
    import geotrellis.vector.io.wkt._
    import geotrellis.vector.io.json._
    //val wkt = scala.io.Source.fromInputStream(getClass.getResourceAsStream("data/wkt/huc10-conestoga.wkt")).getLines.mkString
    val wkt = scala.io.Source.fromFile("data/wkt/huc10-conestoga.wkt").getLines.mkString
    val huc10 = WKT.read(wkt).asInstanceOf[MultiPolygon]
    val layout = TileLayout(3,3,256,256)
    val ld = LayoutDefinition(huc10.envelope, layout)
    val polyRdd = sc.parallelize(huc10.polygons)
    */

    //val polyRdd = sc.parallelize(polys.polygons)
    val polyRdd = sc.parallelize(polys)
    //val polyRdd = sc.parallelize(multipolygons)
    val shp = ShapeFileReader.readSimpleFeatures(shapefileName) //.asInstanceOf[MultiPolygon]
    //val poly = shp.map( feat => feat.getDefaultGeometry.asInstanceOf[MultiPolygon])
    //val polyRdd = sc.parallelize(poly.polygons)
/*
    val layout = TileLayout(3,3,256,256)
    val ld = LayoutDefinition(multipolygons_extent.envelope, layout)
*/
    // multipolygons_extent = Extent(392772.59646032075, 5274511.205333906, 436472.14321178175, 5387308.458370259)
    /*
    //TileLayerMetadata(uint8raw,GridExtent(Extent(429952.23414966895, 5376095.85877421, 437118.8478706409, 5384286.274260508),1.9996132033961982,1.9996131558343677),Extent(429952.23414966895, 5376289.277506294, 436949.68094541645, 5384286.274260508),geotrellis.proj4.CRS$$anon$1@ada07d9,KeyBounds(SpatialKey(0,0),SpatialKey(13,15)))
    TileLayerMetadata(
    cellType = uint8raw,
    layout = GridExtent(
            Extent(429952.23414966895, 5376095.85877421, 437118.8478706409, 5384286.274260508),
            1.9996132033961982,1.9996131558343677),
    extent = Extent(429952.23414966895, 5376289.277506294, 436949.68094541645, 5384286.274260508),
    crs = geotrellis.proj4.CRS$$anon$1@ada07d9,
    bounds = KeyBounds(SpatialKey(0,0),SpatialKey(13,15)))
     */
    val metadata : TileLayerMetadata[SpatialKey] = inputRdd.metadata
    val m_cellType : geotrellis.raster.CellType = metadata.cellType
    val m_layout : geotrellis.spark.tiling.LayoutDefinition = metadata.layout
    val m_extent : geotrellis.vector.Extent = metadata.extent
    val m_crs : geotrellis.proj4.CRS = metadata.crs
    val m_bounds : geotrellis.spark.Bounds[SpatialKey] = metadata.bounds
    val m_crs_proj4 : org.osgeo.proj4j.CoordinateReferenceSystem = m_crs.proj4jCrs
    val m_crs_proj4_string : String = m_crs_proj4.getParameterString
    val m_crs_proj4_param : Array[String] = m_crs_proj4.getParameters
    val m_crs_proj4_proj : org.osgeo.proj4j.proj.Projection = m_crs_proj4.getProjection
    val m_crs_epsilon : Double = m_crs.Epsilon
    //TileLayout(14,16,256,256)
    //Extent(429952.23414966895, 5376289.277506294, 436949.68094541645, 5384286.274260508)
    val layout = inputRdd.metadata.layout.tileLayout
    val extent = inputRdd.metadata.extent
    val ld = LayoutDefinition(extent, layout)

    /*
    val rasterizedRDD : RDD[(SpatialKey, Tile)] with Metadata[LayoutDefinition] = RasterizeRDD
      .fromGeometry(
        polyRdd,
        1,
        IntConstantNoDataCellType,
        ld,
        Options.DEFAULT)
        // */
    //val rasterizedRdd = polyRdd.rasterizeWithValue(1, IntConstantNoDataCellType, ld)

    //*
    //val valuesRdd = sc.parallelize(values)
    val multipolygonsRdd = sc.parallelize(multipolygons)
    val rasterizedRDD : RDD[(SpatialKey, Tile)] with Metadata[LayoutDefinition] = RasterizeFeaturesRDD
      .fromFeature(
        multipolygonsRdd,
        IntConstantNoDataCellType,
        ld,
        Options.DEFAULT)
    // */

    // TODO: figure out how to extract the layoutScheme from the inputRdd
    //val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = Utils.TILE_SIZE)
    val layoutScheme = FloatingLayoutScheme(tileSize = Utils.TILE_SIZE)
    //val (zoomlevel, output) = rasterizedRDD.reproject(zoomedLayoutScheme = ZoomedLayoutScheme(WebMercator))
//    val crs = WebMercator
//    val myRasterMetaData = TileLayerMetadata(IntConstantNoDataCellType, ld, multipolygons_extent, crs, bounds = geotrellis.spark.Bounds(3,3))
    //val (zoom, reprojected) =
    //  TileLayerRDD(rasterizedRDD, myRasterMetaData)
    //    .reproject(crs, layoutScheme, Utils.RESAMPLING_METHOD)

    val rasterizedRDDwithContext : RDD[(SpatialKey,Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
    //  TileLayerRDD(rasterizedRDD, myRasterMetaData)
        TileLayerRDD(rasterizedRDD, inputRdd.metadata)

    val zoomLevel = 0
    val layerId = LayerId(layerName, zoomLevel)
    if (!attributeStore.layerExists(layerId)) {
      //layerWriter.write(layerId, rasterizedRDD, ZCurveKeyIndexMethod)
      layerWriter.write(layerId, rasterizedRDDwithContext, ZCurveKeyIndexMethod)
    }

    /*
    val test = multipolygons  //: List[(List[Polygon],Int)]
      .map { feat =>
        val (mp, value) = feat
        (mp.polygons.toList, value)
      }
      //.flatMap{ case()}
    val test2 = test
      .flatMap{ x =>
        (x._1)
      }
      */

    //val polygons = multipolygons  //: List[(Polygon, Int)]
    //  .flatMap{ feat => feat._1.polygons.map( poly => (poly, feat._2)) }
    /*
    val polygons = multipolygons  //: List[(Polygon,Int)]
        .map{ feat => //case (mp: MultiPolygon, value: Int) =>
            //val (mp, value) = feat
            val mp = feat._1
            val value = feat._2
            val first = mp.polygons.reduce( (left, right) => left)
            val last = mp.polygons.last
            val intval = value
            (first, intval)}
    */
    /*val rasterized = multipolygons.map{ feat =>
      val polygon0 = feat._1.polygons(0)
      val rasterized = polygon0.rasterize( RasterExtent(multipolygons_extent, 100, 100))
      rasterized
    }*/
    /*
    polygons.foreach{ feat => // case(poly: Polygon, value: Int) =>
      val (poly, value) = feat
      println(poly.vertexCount)
      //rasterize.polygon.PolygonRasterizer.foreachCellByPolygon()
    }
    */


    /*
    val shp = ShapeFileReader.readSimpleFeatures(shapefileName)
    val mps: Seq[MultiPolygonFeature[Int]] =
      for(ft <- shp) yield{
        val ID : String = ft.getID
        val geomdef = ft.getDefaultGeometry
        val geom = ft.getAttribute(0).asInstanceOf[jts.MultiPolygon]
        val attribs = ft.getAttributes
        attribs.remove(0)
        val props: Map[String, Object] =
          ft.getProperties.asScala.drop(1).map { p =>
            (p.getName.toString, ft.getAttribute(p.getName))
          }.toMap
        val data = props(attribName).asInstanceOf[Long].toInt
        //val data = props(ft.getProperties.asScala.last.getName.toString).asInstanceOf[Long].toInt
        println(ID)
        println(geomdef)
        println(attribs)
        println(data)
        println(props)
        MultiPolygonFeature(geom, data)
      }
    */
    //ToDo: Get Shapefile Context (JTS Geometry Format) into Spark Context

    /*
    val extent = {
      val env = mps.map(_.geom.envelope).reduce(_ union _).asInstanceOf[jts.Envelope]
      Extent(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
    }
    val re = RasterExtent(extent, 255, 255)
    */

    /*
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

    import scala.collection.JavaConverters._
    import com.vividsolutions.jts.{geom => jts}

    /*
    //val shp = ShapeFileReader.readSimpleFeatures("data/demographics.shp")
    val mps: Seq[MultiPolygon[Int]] =
      for (ft <- shp) yield {
        val geom = ft.getAttribute(0).asInstanceOf[jts.MultiPolygon]
        val props: Map[String, Object] =
          ft.getProperties.asScala.drop(1).map { p =>
            (p.getName.toString, ft.getAttribute(p.getName))
          }.toMap
        val data = props("bedeckung").asInstanceOf[Long].toInt
        new MultiPolygon(geom, data)
      }

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
    // */

  }
}
