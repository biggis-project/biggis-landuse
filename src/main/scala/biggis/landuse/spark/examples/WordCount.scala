package biggis.landuse.spark.examples

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{SparkContext, SparkException}

object WordCount extends LazyLogging {
  def main(args: Array[String]): Unit = {
    try {
      val Array(input, output) = args
      implicit val sc : SparkContext = Utils.initSparkAutoContext
      WordCount()(input, output, sc)
      sc.stop()
    }
    catch {
      case _: MatchError => println("Run as: /path/to/input, /path/to/output")
      case e: SparkException => logger error e.getMessage
    }
  }

  def apply()(implicit input: String, output: String, sc: SparkContext): Unit = {

    val textFile = sc.textFile(input)
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(output)
  }
}