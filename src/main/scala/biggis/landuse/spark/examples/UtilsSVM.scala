package biggis.landuse.spark.examples

import geotrellis.raster.MultibandTile
import geotrellis.spark.SpatialKey
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
  * Created by ak on 01.12.2016.
  */
object UtilsSVM {

  def MultibandTile2LabelPoint (rdd : RDD[(SpatialKey, MultibandTile)]): RDD[LabeledPoint] = {
    // ToDo: Select only pixels within training data
    val tile: MultibandTile = rdd
      .distinct()
      .stitch()
    val arrayLP : Array[LabeledPoint] = Array.ofDim[LabeledPoint](tile.rows * tile.cols)
    for( y <- 0 until tile.rows; x <- 0 until tile.cols){
      val bandbuffer = Array.fill(tile.bandCount)(0.0)  // Array[Double]
      for(bandno <- 0 until tile.bandCount){
        bandbuffer(bandno) = tile.band(bandno).get(x,y)
      }
      val bandVector = Vectors.dense(bandbuffer)  // org.apache.spark.mllib.linalg.Vector
      val classid : Double = 0.0  // ToDo: Apply class id here
      val labeledPixel = LabeledPoint(classid,bandVector.compressed)
      arrayLP(y * tile.cols + x) = labeledPixel
    }
    val data_temp: RDD[LabeledPoint] = rdd.sparkContext
      .parallelize( arrayLP
        .filter( _.features.numNonzeros > 0 )
        .map( l => l ) )
    data_temp
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
