package biggis.landuse.spark.examples

import java.net.UnknownHostException

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object UploadToHdfs extends App with LazyLogging {

  try {
    val Array(localPath, hdfsPath, hdfsUrl) = args
    UploadToHdfs(localPath, hdfsPath, hdfsUrl)
  } catch {
    case _: MatchError => println("Run as: localPath hdfsPath hdfsUrl")
    case e: IllegalArgumentException => {
      e.getCause match  {
        case _ : UnknownHostException => logger error s"Unknown HDFS host, try hdfs://localhost:8020"
        case _ => logger error e.getMessage
      }
    }
  }

  def apply(localPath: String, hdfsPath: String, hdfsUrl: String): Unit = {
    logger info s"Uploading local file $localPath to hdfs $hdfsPath ..."

    val conf = new Configuration() {
      set("fs.default.name", hdfsUrl)
    }
    val fileSystem = FileSystem.get(conf)
    fileSystem.copyFromLocalFile(new Path(localPath), new Path(hdfsPath))

    logger info "done"
  }

}