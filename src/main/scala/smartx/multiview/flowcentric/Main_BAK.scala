/*
package smartx.multiview.flowcentric

import java.time.LocalDateTime

//import com.mongodb.spark._
//import com.mongodb.spark.config._scala
//import com.mongodb.spark.sql._
import java.io.File
import java.util.regex.Pattern

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
//import java.nio.file.{ Files, Path, StandardCopyOption }
//import org.elasticsearch.spark.sql._

object Main {

  def main(args: Array[String]) {




    val end_time = LocalDateTime.now().minusMinutes(1)
    val start_time = LocalDateTime.now().minusMinutes(6)
    val processing_time = current_timestamp()

    //Create a SparkContext to initialize Spark
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Flow-centric Validation Tagging Aggregation")

      .getOrCreate()
    import spark.implicits._

    //Read latest packets file from directory
    def getListOfFiles(dir: String): List[File] = {
      val d = new File(dir)
      if (d.exists.&&(d.isDirectory)) {
        d.listFiles.filter(_.isFile).toList
      } else {
        scala.List[File]()
      }
    }

    //Remove previous data files from the disk
    val dir = new File("/opt/Multi-View-Staged/")
    //FileUtils.deleteDirectory(dir)



    AggregateControlPlane(spark)
    spark.close()




    //Control Plane network packets data processing
    def AggregateControlPlane(spark: SparkSession): Unit = {
      //Load network packets data from disk. Validate network packets data according to the defined structure
      //val files = getListOfFiles("/opt/IOVisor-Data/Control-Plane-Latest/")
      val files = getListOfFiles("/opt/IOVisor-Data/flows/")
      println(files)

      val filesName = new File("/opt/IOVisor-Data/flows/").list.toList // gives list of file names including extensions in the path `path`
      println(filesName)

      val out_path ="/opt/IOVisor-Data/flowsOut/"
      for (f <- filesName) {
        val p = Pattern.compile("(.+?)(\\.[^.]*$|$)") // regex to identify files names and extensions
        val m = p.matcher(f)

        if (m.find()) {

        }

        }
      //Create an empty data set
      var allPackets = Seq.empty[DataFrame]

      //Read all the network packets files for last 5 minutes
      for (file <- files) {
        val packetFile = spark.read.format("csv").option("header", "false").load(file.toString)
        val packets = packetFile.map(p => ValidateControlPlane.apply(p(0).toString.toDouble, p(1).toString, p(2).toString, p(3).toString, p(4).toString.toInt, p(5).toString, p(6).toString, p(7).toString.toInt, p(8).toString.toInt, p(9).toString.toInt, p(10).toString.toInt, p(11).toString.toInt)).toDF()
        allPackets = allPackets :+ packets
      }

      //Combine all data frames together
      var FinalPackets = allPackets.reduce(_.union(_))

      //Remove unnecessary fields
      val selectResult = FinalPackets.select("collectiontime", "measurementboxname", "src_host", "dest_host", "src_host_port", "dest_host_port", "protocol", "tcp_window_size", "databytes", "net_plane").cache()




      val groupResult = selectResult.groupBy("measurementboxname", "src_host", "dest_host", "src_host_port", "dest_host_port", "protocol", "net_plane")
        .agg(
          count("protocol") as "packets",
          round(min("tcp_window_size"),2) as "mintcpwindowsize",
          round(max("tcp_window_size"),2) as "maxtcpwindowsize",
          round(avg("tcp_window_size"),0) as "avgtcpwindowsize",
          when(stddev("tcp_window_size") === "NaN", 0.0).otherwise(round(stddev("tcp_window_size"), 2)) as "stddevtcpwindowsize",
          round(min("databytes"),2) as "mindatabytes",
          round(max("databytes"), 2) as "maxdatabytes",
          round(avg("databytes"),2) as "avgdatabytes",
          when(stddev("databytes") === "NaN", 0.0).otherwise(round(stddev("databytes"),2)) as "stddevdatabytes",
          round(sum("databytes"),0) as "totaldatabytes",
          round((max("collectiontime") - min("collectiontime"))/1000000, 3) as "flowduration"
        )


      //Add processing time to data frame
      val result = groupResult.withColumn("processing_time", processing_time).cache()

      //Save result to parquet file format
      //result.toDF.write.csv("/opt/Multi-View-Staged/AggregateControlPlane.csv")
      result.coalesce(1).write.format("csv").save("/opt/Multi-View-Staged/AggregateControlPlane.csv")
/*
      import org.apache.hadoop.fs.FileSystem
      val fs = FileSystem.get(sc.hadoopConfiguration)
      val file = fs.globStatus(new Nothing("path/file.csv/part*"))
*/




      //Remove raw data file from disk
      for (file <- files) {
        Predef.println("Deleted Raw File: " + file.toString)

//        FileUtils.deleteQuietly(new File(file.toString))
      }
    }
  }

  //Define classes to validate collected visibility data

  case class ValidateControlPlane(collectiontime: Double, net_plane: String, measurementboxname: String, measurementboxip: String, ipversion: Int, src_host: String, dest_host: String, src_host_port: Int, dest_host_port: Int, protocol: Int, tcp_window_size: Int, databytes: Int)



}*/
