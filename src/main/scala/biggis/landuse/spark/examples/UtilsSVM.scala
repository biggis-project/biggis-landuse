package biggis.landuse.spark.examples

import geotrellis.raster.{DoubleConstantNoDataCellType, MultibandTile}
import geotrellis.spark.{Metadata, SpatialKey, TileLayerMetadata}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMMultiClassOVAModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods._

/**
  * Renamed by ak on 26.01.2017.
  */
@deprecated("do not use, except for debugging, replace by UtilsML")
object UtilsSVM extends biggis.landuse.spark.examples.UtilsML {
  case class BandNoLabel(classBandNo: Int)
  def MultibandTile2xyLabeledPoint( data : RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] )(implicit classBandNo : BandNoLabel = BandNoLabel(-1) ): RDD[(SpatialKey, (Int, Int, LabeledPoint))] = {
    val samples: RDD[(SpatialKey, (Int, Int, LabeledPoint))] with Metadata[TileLayerMetadata[SpatialKey]] =
      data.withContext { rdd =>
        rdd.flatMapValues(mbtile =>
          UtilsML.MultibandTile2LabeledPixelSamples(mbtile, classBandNo = classBandNo.classBandNo )
        )
      }
    samples
  }
  def MultibandTile2LabeledPoint( data : RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] )(implicit classBandNo : BandNoLabel = BandNoLabel(-1) ): RDD[LabeledPoint] = {
    val samples = MultibandTile2xyLabeledPoint(data)
    val lp = samples
      .map( sample => sample._2._3 )
      .filter(_.features.numNonzeros > 0)
    lp
  }

  def SplitSamples( samples : RDD[LabeledPoint], factor : Double): (RDD[LabeledPoint], RDD[LabeledPoint]) = {
    // Split data into training (60%) and test (40%).
    val splits = samples.randomSplit(Array(factor, 1.0-factor), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    (training,test)
  }

  def SaveSVMClassifier( model_multi : SVMMultiClassOVAModel, svmClassifier : String)(implicit sc : SparkContext): Unit ={
    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
    if (hdfs.exists(new org.apache.hadoop.fs.Path(svmClassifier))) {
      try {
        hdfs.delete(new org.apache.hadoop.fs.Path(svmClassifier), true)
      } catch {
        case _: Throwable =>
      }
    }
    model_multi.save(sc, svmClassifier)
  }

  @deprecated("do not use, replace by UtilsML.MultibandTile2LabeledPixelSamples")
  case class LabelPointSpatialRef(spatialKey: SpatialKey, offset: Int)

  @deprecated("do not use, replace by UtilsML.MultibandTile2LabeledPixelSamples")
  def MultibandTile2LabelPoint(data: (SpatialKey, MultibandTile)): Array[(LabeledPoint, LabelPointSpatialRef)] = {

    val (spatialKey, tile) = data

    val arrayLP = Array.ofDim[(LabeledPoint, LabelPointSpatialRef)](tile.rows * tile.cols)

    for (y <- 0 until tile.rows; x <- 0 until tile.cols) {
      val bandbuffer = Array.fill(tile.bandCount)(0.0) // Array[Double]
      for (bandno <- 0 until tile.bandCount) {
        bandbuffer(bandno) = tile.band(bandno).get(x, y)
      }

      val bandVector = Vectors.dense(bandbuffer)
      // org.apache.spark.mllib.linalg.Vector
      val classid: Double = 0.0
      // TODO: take class from the vector data
      // ToDo: Apply class id here
      val labeledPixel = LabeledPoint(classid, bandVector.compressed)
      val offset = y * tile.cols + x
      arrayLP(offset) = (labeledPixel, LabelPointSpatialRef(spatialKey, offset))
    }
    arrayLP
  }

  @deprecated("do not use, replace by UtilsML.MultibandTile2LabeledPixelSamples")
  def MultibandTile2LabelPoint(rdd: RDD[(SpatialKey, MultibandTile)]): (RDD[LabeledPoint], RDD[LabelPointSpatialRef]) = {
    // ToDo: Select only pixels within training data
    val data_temp_with_spatialref: RDD[(LabeledPoint, LabelPointSpatialRef)] = rdd
      .flatMap(tile => MultibandTile2LabelPoint(tile))
      .filter(_._1.features.numNonzeros > 0)
    val data_temp: RDD[LabeledPoint] = data_temp_with_spatialref
      .map(item => item._1)
    val spatialref: RDD[LabelPointSpatialRef] = data_temp_with_spatialref
      .map(item => item._2)
    (data_temp, spatialref)
  }

  @deprecated("do not use, replace by UtilsML.SaveAsLibSVMFile")
  def SaveAsLibSVMFile(data: (RDD[LabeledPoint], RDD[LabelPointSpatialRef]), trainingName: String): Unit = {
    try {
      UtilsML.SaveAsLibSVMFile(data._1, trainingName)
    }
    catch {
      case _: Throwable =>
    }
  }

  @deprecated("for debugging purposes")
  case class Delimiter(delimiter: String)
  @deprecated("for debugging purposes")
  def SaveAsCSVFile(data: RDD[LabeledPoint], trainingName: String, delimiter: Delimiter = Delimiter(";"))(implicit removeZeroLabel: Boolean = false): Unit = {
    try {
      def SaveCSV(data: RDD[LabeledPoint], trainingName: String)(implicit delimiter: Delimiter) : Unit = {
        data
          .map( row => {Array(row.label) ++ row.features.toDense.toArray}.mkString(delimiter.delimiter) )
          .coalesce(1, shuffle = true)
          .saveAsTextFile(trainingName)
      }
      implicit val sc = data.sparkContext
      val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
      val trainingPath = ParsePath(trainingName)
      val first_dir = trainingPath.dir_hierarchy.toArray.apply(1)
      val use_single_file_export = trainingPath.filetype=="csv"
      if(use_single_file_export){
        val trainingNameTemp = trainingName+"_temp"
        DeleteFile(trainingNameTemp)
        if(removeZeroLabel){
          val data_w_o_nodata = data.filter( _.label > 0)
          SaveCSV(data_w_o_nodata, trainingNameTemp)(delimiter)
        } else
          SaveCSV(data, trainingNameTemp)(delimiter)
        DeleteFile(trainingName)
        FileUtil.copyMerge(hdfs, new Path(trainingNameTemp), hdfs, new Path(trainingName), true, sc.hadoopConfiguration, null)
        DeleteFile(trainingNameTemp)
      }
      else {
        DeleteFile(trainingName)
        if(removeZeroLabel){
          val data_w_o_nodata = data.filter( _.label > 0)
          SaveCSV(data_w_o_nodata, trainingName)(delimiter)
        } else
          SaveCSV(data, trainingName)(delimiter)
      }
    }
    catch {
      case _: Throwable =>
    }
  }

  def LabeledPointWithKeyToArray( row :(SpatialKey, (Int, Int, LabeledPoint)) ) : Array[Any] = {
    val (key: SpatialKey, (x: Int, y: Int, lp : LabeledPoint) ) = row
    Array(lp.label) ++ lp.features.toDense.toArray ++ Array(key) ++ Array(x) ++ Array(y)
  }
  def ArrayToLabeledPointWithKey(cols: Array[String]) : (SpatialKey, (Int, Int, LabeledPoint)) = {
    val featurelength = cols.length - 1 - 3   // label[1] + features[featurelength] + keys(SpatialKey,Int,Int)[3]
    val label = cols(0).toDouble
    val features = cols.take(1 + featurelength)
    val keys = cols.drop(1 + featurelength)
    val skey = keys(0).split("SpatialKey(,)")
    val (key : SpatialKey, x: Int, y:Int) = (SpatialKey(skey(0).toInt, skey(1).toInt),keys(1).toInt,keys(2).toInt)
    val featuresWithoutLabel = features.drop(1).map( col => col.toDouble ).toIterable
    val featuresMllib = Vectors.dense(featuresWithoutLabel.toArray).compressed
    (key,(x,y, LabeledPoint(label, featuresMllib)))
  }
  def LabeledPointWithKeyToString(row :(SpatialKey, (Int, Int, LabeledPoint)))(implicit delimiter: Delimiter) : String = {
    LabeledPointWithKeyToArray(row).mkString(delimiter.delimiter)
  }
  def StringToLabeledPointWithKey(line: String)(implicit  delimiter: Delimiter) : (SpatialKey, (Int, Int, LabeledPoint)) = {
    val cols = line.split(delimiter.delimiter).map(_.trim)
    ArrayToLabeledPointWithKey(cols)
  }

  //@deprecated("for debugging purposes")
  //case class RDDKeyLabeledPoint( rdd : RDD[(SpatialKey, (Int, Int, LabeledPoint))] with Metadata[TileLayerMetadata[SpatialKey]])
  @deprecated("for debugging purposes")
  def SaveAsCSVFileWithKey(data: RDD[(SpatialKey, (Int, Int, LabeledPoint))] with Metadata[TileLayerMetadata[SpatialKey]], trainingName: String, delimiter: Delimiter = Delimiter(";")): Unit = {
    try {
      def SaveCSV(data: RDD[(SpatialKey, (Int, Int, LabeledPoint))] with Metadata[TileLayerMetadata[SpatialKey]], trainingName: String)(implicit delimiter: Delimiter) : Unit = {
        data
          .filter(_._2._3.features.numNonzeros > 0) //Avoid NoData
          .map( row => LabeledPointWithKeyToString(row))  //LabeledPointWithKeyToArray(row).mkString(delimiter.delimiter))
          .coalesce(1, shuffle = true)
          .saveAsTextFile(trainingName)
      }
      def SaveCSVMetadata(data: RDD[(SpatialKey, (Int, Int, LabeledPoint))] with Metadata[TileLayerMetadata[SpatialKey]], trainingMetaName: String)(implicit delimiter: Delimiter) : Unit = {
        val metadata = data.metadata
        val cellType = metadata.cellType
        val layout = metadata.layout
        val extent = metadata.extent
        val crs = metadata.crs.toWKT()
        val bounds =  metadata.bounds.mkString(",")
        val array : Array[String] = Array(cellType.toString()) ++ Array(layout.toString()) ++ Array(extent.toString()) ++ Array(crs.toString()) ++ Array(bounds.toString())
        val serialized_csv : String = array.mkString(delimiter.delimiter)
        //val json = array.toList
        val json = ("extent" -> extent.toString()) ~
          ("layoutDefinition" -> layout.toString()) ~
          ("bounds" -> bounds.toString()) ~
          ("cellType" -> cellType.toString()) ~
          ("crs" -> crs.toString())
        val serialized_json = compact(render(json))
        def writeTextFile(text: String, filename: String)(implicit sc : SparkContext) = {
          // Hadoop Config is accessible from SparkContext
          val fs = FileSystem.get(sc.hadoopConfiguration)
          // Output file can be created from file system.
          val output = fs.create(new Path(filename))
          // But BufferedOutputStream must be used to output an actual text file.
          output.write(text.getBytes("UTF-8"))
          output.close()
        }
        //writeTextFile(serialized_csv, trainingMetaName)(data.sparkContext)
        writeTextFile(serialized_json, trainingMetaName)(data.sparkContext)
      }
      implicit val sc = data.sparkContext
      val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
      val trainingPath = ParsePath(trainingName)
      val first_dir = trainingPath.dir_hierarchy.toArray.apply(1)
      val use_single_file_export = trainingPath.filetype=="csv"
      if(use_single_file_export){
        val trainingNameTemp = trainingName+"_temp"
        DeleteFile(trainingNameTemp)
        SaveCSV(data, trainingNameTemp)(delimiter)
        DeleteFile(trainingName)
        FileUtil.copyMerge(hdfs, new Path(trainingNameTemp), hdfs, new Path(trainingName), true, sc.hadoopConfiguration, null)
        DeleteFile(trainingNameTemp)
        SaveCSVMetadata(data, trainingName+".metadata")(delimiter)
      }
      else {
        DeleteFile(trainingName)
        SaveCSV(data, trainingName)(delimiter)
        SaveCSVMetadata(data, trainingName+".metadata")(delimiter)
      }
    }
    catch {
      case _: Throwable =>
    }
  }

  @deprecated("for debugging purposes")
  def LoadFromCSVFileWithKey(fileNameCSV: String, delimiter: Delimiter = Delimiter(";"))(implicit sc : SparkContext): Unit = {//} RDD[(SpatialKey, (Int, Int, LabeledPoint))] with Metadata[TileLayerMetadata[SpatialKey]] = {
    try {
      def ToRDD(data : Iterator[(SpatialKey, (Int, Int, LabeledPoint))]) : RDD[(SpatialKey, (Int, Int, LabeledPoint))] = {
        val dataRDD : RDD[(SpatialKey, (Int, Int, LabeledPoint))] = sc.parallelize(data.toSeq, 1)
        dataRDD
      }
      def LoadCSV( trainingName: String)(implicit delimiter: Delimiter) : RDD[(SpatialKey, (Int, Int, LabeledPoint))] = { //with Metadata[TileLayerMetadata[SpatialKey]]
        //println("Label, Features, ..., SpatialKey, x, y")
        val bufferedSource = scala.io.Source.fromFile(trainingName)
        val data = bufferedSource.getLines()
          .map( line => StringToLabeledPointWithKey(line))
        bufferedSource.close
        ToRDD(data)
      }
      def LoadCSVMetadata(trainingMetaName: String)(implicit delimiter: Delimiter) : Array[String] ={ // : Metadata[TileLayerMetadata[SpatialKey]] = {
        val bufferedSource = scala.io.Source.fromFile(trainingMetaName)
        def StringToMetadata( text : String) : String = {
          val json = parse(text)
          text
        }
        val metadata : Iterator[String] = bufferedSource.getLines()
          .map( line => StringToMetadata(line))
        bufferedSource.close
        metadata.toArray
      }
      val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
      val data : RDD[(SpatialKey, (Int, Int, LabeledPoint))] =
      if (hdfs.exists(new org.apache.hadoop.fs.Path(fileNameCSV))){
        val metadata = LoadCSVMetadata(fileNameCSV+".metadata")(delimiter)
        LoadCSV(fileNameCSV)(delimiter)
      }
      else {
        val empty = Array[String]()
        val data = empty
          .map( line => StringToLabeledPointWithKey(line)(delimiter))
        ToRDD(data.toIterator)
      }
    }
    catch {
      case _: Throwable =>
    }
    //val data: RDD[(SpatialKey, (Int, Int, LabeledPoint))] with Metadata[TileLayerMetadata[SpatialKey]] = ()
    //data
  }

  def toKafkaString( samples : RDD[(SpatialKey, (Int, Int, LabeledPoint))], delimiter: Delimiter = Delimiter(";")) : RDD[(String)] = {
    samples
      .filter( samples => samples._2._3.features.numNonzeros > 0) //Avoid NoData
      .map( sample => {
      val (key, (x,y, lp)) : (SpatialKey, (Int, Int, LabeledPoint)) = sample
      val str = UtilsSVM.LabeledPointWithKeyToString(sample)(delimiter)
      str
    }
    )
  }

  def NormalizeMultibandTile( data : RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] )(implicit classBandNo : BandNoLabel = BandNoLabel(-1)): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
    val normalizedtiles: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] =
      data
        .withContext { rdd => {
          //find minMax per band
          val (band_min, band_max): (Array[Double], Array[Double]) = /*{
            rdd.map( mbtile => {
              val (key, tile) = mbtile
              var local_band_min: List[Double] = List[Double]()//Array[Double] = Array[Double](nbands)
              var local_band_max: List[Double] = List[Double]()//Array[Double] = Array[Double](nbands)
              tile.bands.foreach(band => {
                val (min, max) = band.findMinMaxDouble
                local_band_min = local_band_min ++ List[Double](min)
                local_band_max = local_band_max ++ List[Double](max)
              })
              (local_band_min.toArray[Double],local_band_max.toArray[Double])
            })
              .reduce( (left, right) => {
                val (left_min,left_max) = left
                val (right_min,right_max) = right
                val nbands = left_min.size
                var zonal_band_min: Array[Double] = Array.fill[Double](nbands)(Double.MaxValue)
                var zonal_band_max: Array[Double] = Array.fill[Double](nbands)(Double.MinValue)
                for(i <- 0 until nbands){
                  zonal_band_min.update(i,Math.min(left_min(i),right_min(i)))
                  zonal_band_max.update(i,Math.max(left_max(i),right_max(i)))
                }
                (zonal_band_min,zonal_band_max)
              })
          }*/
          findMinMaxDoubleMultiband(rdd)
          //normalize
          rdd
            .mapValues { tile => {
              tile.convert(DoubleConstantNoDataCellType).mapBands { case (i, band) =>
                if(classBandNo.classBandNo == i) band //do not normalize class band
                else band.normalize(band_min(i),band_max(i),0.0,1.0)
                }
            }
            }
        }
        }
    normalizedtiles
  }

  def findMinMaxDoubleMultiband( rdd: RDD[(SpatialKey,MultibandTile)]) : (Array[Double],Array[Double]) = {
    //find minMax per band
    val (band_min, band_max): (Array[Double], Array[Double]) = {
      rdd.map( mbtile => {
        val (key, tile) = mbtile
        var (local_band_min, local_band_max): (Array[Double], Array[Double]) = (Array[Double](), Array[Double]())
        tile.bands.foreach(band => {
          val (min, max) = band.findMinMaxDouble
          local_band_min = local_band_min ++ Array[Double](min)
          local_band_max = local_band_max ++ Array[Double](max)
        })
        (local_band_min,local_band_max)
      })
        .reduce( (left, right) => {
          val ((left_min,left_max),(right_min,right_max)) = (left, right)
          val nbands = left_min.size
          var (zonal_band_min,zonal_band_max): (Array[Double],Array[Double]) = (Array.fill[Double](nbands)(Double.MaxValue),Array.fill[Double](nbands)(Double.MinValue))
          for(i <- 0 until nbands){
            zonal_band_min.update(i,Math.min(left_min(i),right_min(i)))
            zonal_band_max.update(i,Math.max(left_max(i),right_max(i)))
          }
          (zonal_band_min,zonal_band_max)
        })
    }
    (band_min,band_max)
  }
}
