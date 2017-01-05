package biggis.landuse.spark.examples

import geotrellis.raster.MultibandTile
import geotrellis.spark.{SpaceTimeKey, SpatialKey, TemporalKey}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
  * Created by ak on 01.12.2016.
  */
object UtilsSVM {
  case class LabelPointSpatialRef(spatialKey: SpatialKey, offset: Int)
  def MultibandTile2LabelPoint (rdd : RDD[(SpatialKey, MultibandTile)]): (RDD[LabeledPoint], RDD[LabelPointSpatialRef]) = {
    // ToDo: Select only pixels within training data
    val tiles = rdd
      .distinct()
      .collect()
    // ToDo: Avoid using Array as Buffer, map directly using .collect.foreach() ?!
    val arrayLPbuffer : Array[Array[(LabeledPoint,LabelPointSpatialRef)]] = Array.ofDim[(LabeledPoint, LabelPointSpatialRef)]( tiles.length, tiles.array(0)._2.rows * tiles.array(0)._2.cols)
    for(tileno <- tiles.indices){
      val spatialKey : SpatialKey = tiles(tileno)._1
      val tile: MultibandTile = tiles(tileno)._2
      val arrayLP : Array[(LabeledPoint,LabelPointSpatialRef)] = Array.ofDim[(LabeledPoint,LabelPointSpatialRef)](tile.rows * tile.cols)
      for( y <- 0 until tile.rows; x <- 0 until tile.cols){
        val bandbuffer = Array.fill(tile.bandCount)(0.0)  // Array[Double]
        for(bandno <- 0 until tile.bandCount){
          bandbuffer(bandno) = tile.band(bandno).get(x,y)
        }
        val bandVector = Vectors.dense(bandbuffer)  // org.apache.spark.mllib.linalg.Vector
        val classid : Double = 0.0  // ToDo: Apply class id here
        val labeledPixel = LabeledPoint(classid,bandVector.compressed)
        val offset = y * tile.cols + x
        arrayLP(offset) = (labeledPixel, LabelPointSpatialRef(spatialKey, offset))
      }
      arrayLPbuffer(tileno) = arrayLP
    }
    val data_temp_with_spatialref: RDD[(LabeledPoint,LabelPointSpatialRef)] = rdd.sparkContext
      .parallelize( arrayLPbuffer
        .flatMap( tile => tile
          .filter( _._1.features.numNonzeros > 0 )
          .map( l => ( l._1, LabelPointSpatialRef(l._2.spatialKey, l._2.offset)) ) ) )
    val data_temp : RDD[LabeledPoint] = data_temp_with_spatialref
      .map( item => item._1 )
    val spatialref : RDD[LabelPointSpatialRef] = data_temp_with_spatialref
      .map( item => item._2 )
    (data_temp, spatialref)
  }

  def SaveAsLibSVMFile (data : (RDD[LabeledPoint],RDD[LabelPointSpatialRef]), trainingName : String): Unit ={
    try{
      val hdfs = org.apache.hadoop.fs.FileSystem.get(data._1.sparkContext.hadoopConfiguration)
      if(hdfs.exists(new org.apache.hadoop.fs.Path(trainingName))){
        try { hdfs.delete(new org.apache.hadoop.fs.Path(trainingName), true)} catch { case _ : Throwable =>  }
      }
      MLUtils.saveAsLibSVMFile(data._1, trainingName)
    }
    catch{ case _ : Throwable => }
  }
  def SaveAsLibSVMFile (data : RDD[LabeledPoint], trainingName : String): Unit ={
    try{
      val hdfs = org.apache.hadoop.fs.FileSystem.get(data.sparkContext.hadoopConfiguration)
      if(hdfs.exists(new org.apache.hadoop.fs.Path(trainingName))){
        try { hdfs.delete(new org.apache.hadoop.fs.Path(trainingName), true)} catch { case _ : Throwable =>  }
      }
      MLUtils.saveAsLibSVMFile(data, trainingName)
    }
    catch{ case _ : Throwable => }
  }
}
