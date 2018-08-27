package biggis.landuse.spark.examples

import geotrellis.util.LazyLogging
import geotrellis.shapefile.ShapeFileReader
import geotrellis.shapefile.ShapeFileReader.SimpleFeatureWrapper
import geotrellis.vector.{Extent, Feature, MultiPolygon}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._
import com.vividsolutions.jts.{geom => jts}
import geotrellis.proj4.CRS
import geotrellis.vector.io.json.JsonFeatureCollection
import geotrellis.vector.io.json._
import geotrellis.vector.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.spark.SparkContext
import spray.json._
import spray.json.DefaultJsonProtocol

/**
  * Created by ak on 22.06.2017.
  */
object UtilsShape extends LazyLogging{

  def readShapefileMultiPolygonLongAttribute(shapefileName: String, attribName: String)(implicit targetcrs : Option[CRS] = None, sc: SparkContext): List[Feature[MultiPolygon,Long]] = {
    if (shapefileName.contains(".shp")) {
      ShapeFileReader.readSimpleFeatures(shapefileName)
        .filter { feat =>
          "MultiPolygon" != feat.getFeatureType.getGeometryDescriptor.getType.getName.toString
        }
        .map { feat =>
          Feature(MultiPolygon.jts2MultiPolygon(feat.geom[jts.MultiPolygon].get), feat.attribute(attribName))
        }
        .toList
    }
    else if(shapefileName.contains(".geojson")){
      readGeoJSONMultiPolygonLongAttribute(shapefileName, attribName)
    } else {
      List[Feature[MultiPolygon,Long]]()
    }
  }
  def readGeoJSONMultiPolygonLongAttribute(geojsonName: String, attribName: String)(implicit targetcrs : Option[CRS] = None, sc: SparkContext): List[Feature[MultiPolygon,Long]] = {
    if(geojsonName.contains(".geojson")){
      val collection = fromFileHdfs[WithCrs[JsonFeatureCollection]](geojsonName)//GeoJson.fromFile[WithCrs[JsonFeatureCollection]](geojsonName)  //Source.fromFile(geojsonName, "UTF-8").mkString.parseGeoJson[WithCrs[JsonFeatureCollection]]

      case class Landcover(landcover: Long)
      object UtilsShapeJsonProtocol extends DefaultJsonProtocol {
        implicit val landcoverValue = jsonFormat(Landcover, attribName) //"bedeckung")
      }
      import  UtilsShapeJsonProtocol._
      //val poly = collection.obj.getAllPolygons()
      val jsoncrs = collection.crs
      val crsoption = jsoncrs.toCRS
      val pattern = ("^" + "(.*)" + "\\(" + "(.*)" + "(EPSG\\:)" + "(?:\\:)" + "([0-9]*)" + "\\)" + "$").r
      val pattern(crstype, urn_ogc, epsgtype, epsgcode) = jsoncrs.toString
      val epsg = epsgtype + epsgcode //"EPSG:32632"
      val crsepsg = CRS.fromName(epsg)
      val crs : CRS = crsoption.getOrElse(CRS.fromName(epsg))
      logger info s"CRS: ${crs.toString()}"

      val collectionFeatures = collection.obj.getAllPolygonFeatures[Landcover]
        .map { feat =>
          val geom = MultiPolygon(Seq(feat.geom))
          val data : Long = feat.data.landcover
          Feature( if(targetcrs.nonEmpty) geom.reproject(crs,targetcrs.get) else geom, data)  //Feature(geom, data)
        }
      collectionFeatures.toList
    }
    else {
      List[Feature[MultiPolygon,Long]]()
    }
  }
  def fromFileHdfs[T: JsonReader](path: String)(implicit sc: SparkContext) = {
    val src = openHdfs(new Path(path)) //val src = scala.io.Source.fromFile(path)
    val txt =
      try {
        scala.io.Source.fromInputStream(src).mkString
      } finally {
        src.close
      }
    GeoJson.parse[T](txt)
  }
  def openHdfs(path: Path)(implicit sc: SparkContext): FSDataInputStream = {
    val conf: Configuration = sc.hadoopConfiguration
    val fs = path.getFileSystem(conf)
    val valid = fs.exists(path)
    val isFile = fs.isFile(path)
    val isDir = fs.isDirectory(path)
    val src = if(isDir){
      // Fix Ingest into HDFS issue (directory is created for each file with same name as file)
      val status = fs.listStatus(path) //val status = fs.getStatus(pathHdfs)
      val filelist = status.map( file => file.getPath ) //fs.listFiles(pathHdfs,false)
      val file = if(filelist.length == 1) Some(fs.open(filelist(0))) else None
      if(file.nonEmpty)
        file.get // Open file in hdfs (contained in dir with same name)
      else
        fs.open(path) // Unhandled - will cause exception
    } else{
      fs.open(path) // Open file in hdfs
    }
    src
  }

  def readShapefileMultiPolygonDoubleAttribute(shapefileName: String, attribName: String)(implicit targetcrs : Option[CRS] = None, sc: SparkContext): List[Feature[MultiPolygon,Double]] = {
    readShapefileMultiPolygonLongAttribute(shapefileName, attribName).map{ feature  => Feature(feature.geom,feature.data.toDouble) }
  }
  def getExtent(mps : List[Feature[MultiPolygon,Any]]) : Extent = {
    mps
      .map{ feature => feature.geom.envelope }
      .reduce( (a,b) =>
        Extent(
          Math.min(a.xmin, b.xmin),
          Math.min(a.ymin, b.ymin),
          Math.max(a.xmax, b.xmax),
          Math.max(a.ymax, b.ymax)
        )
      ) }
}
