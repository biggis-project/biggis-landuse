package biggis.landuse.spark.examples

import geotrellis.util.LazyLogging
import geotrellis.shapefile.ShapeFileReader
import geotrellis.shapefile.ShapeFileReader.SimpleFeatureWrapper
import geotrellis.vector.{Extent, MultiPolygon}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._
import com.vividsolutions.jts.{geom => jts}

/**
  * Created by ak on 22.06.2017.
  */
object UtilsShape extends LazyLogging{

  def readShapefileMultiPolygonLongAttribute(shapefileName: String, attribName: String): List[(MultiPolygon,Long)] = {
    ShapeFileReader.readSimpleFeatures(shapefileName)
      .filter { feat =>
        "MultiPolygon" != feat.getFeatureType.getGeometryDescriptor.getType.getName
      }
      .map { feat =>
        (MultiPolygon.jts2MultiPolygon(feat.geom[jts.MultiPolygon].get), feat.attribute(attribName))
      }
  }

  def readShapefileMultiPolygonDoubleAttribute(shapefileName: String, attribName: String): List[(MultiPolygon,Double)] = {
    readShapefileMultiPolygonLongAttribute(shapefileName, attribName).map{ case (mp: MultiPolygon, value: Long)  => (mp,value.toDouble) }
  }
  def getExtent(mps : List[(MultiPolygon,Any)]) : Extent = {
    mps
      .map{ case (mp: MultiPolygon,_ : Any) => mp.envelope }
      .reduce( (a,b) =>
        Extent(
          Math.min(a.xmin, b.xmin),
          Math.min(a.ymin, b.ymin),
          Math.max(a.xmax, b.xmax),
          Math.max(a.ymax, b.ymax)
        )
      ) }
}
