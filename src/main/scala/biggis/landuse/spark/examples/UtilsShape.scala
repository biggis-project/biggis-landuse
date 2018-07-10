package biggis.landuse.spark.examples

import geotrellis.util.LazyLogging
import geotrellis.shapefile.ShapeFileReader
import geotrellis.shapefile.ShapeFileReader.SimpleFeatureWrapper
import geotrellis.vector.{Extent, MultiPolygon, Feature}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._
import com.vividsolutions.jts.{geom => jts}
import geotrellis.proj4.CRS
import geotrellis.vector.io.json.JsonFeatureCollection
import geotrellis.vector.io.json._
import geotrellis.vector.io._
import spray.json.DefaultJsonProtocol

/**
  * Created by ak on 22.06.2017.
  */
object UtilsShape extends LazyLogging{

  def readShapefileMultiPolygonLongAttribute(shapefileName: String, attribName: String)(implicit targetcrs : Option[CRS] = None): List[Feature[MultiPolygon,Long]] = {
    if (shapefileName.contains(".shp")) {
      ShapeFileReader.readSimpleFeatures(shapefileName)
        .filter { feat =>
          "MultiPolygon" != feat.getFeatureType.getGeometryDescriptor.getType.getName.toString
        }
        .map { feat =>
          Feature(MultiPolygon.jts2MultiPolygon(feat.geom[jts.MultiPolygon].get), feat.attribute(attribName))
        }
    }
    else if(shapefileName.contains(".geojson")){
      readGeoJSONMultiPolygonLongAttribute(shapefileName, attribName)
    } else {
      List[Feature[MultiPolygon,Long]]()
    }
  }
  def readGeoJSONMultiPolygonLongAttribute(geojsonName: String, attribName: String)(implicit targetcrs : Option[CRS] = None): List[Feature[MultiPolygon,Long]] = {
    if(geojsonName.contains(".geojson")){
      val collection = GeoJson.fromFile[WithCrs[JsonFeatureCollection]](geojsonName)  //Source.fromFile(geojsonName, "UTF-8").mkString.parseGeoJson[WithCrs[JsonFeatureCollection]]

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

  def readShapefileMultiPolygonDoubleAttribute(shapefileName: String, attribName: String)(implicit targetcrs : Option[CRS] = None): List[Feature[MultiPolygon,Double]] = {
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
