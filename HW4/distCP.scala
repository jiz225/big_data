package spark.zjx

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import java.net.URI
import scala.collection.mutable.ArrayBuffer


object distCP {
  case class Options(sourceF: String, targetF: String, maxConcurrence: Int, ignoreFailures: Boolean)
  def main(args: Array[String]): Unit = {
    val appName = "distCP"
    val master = "local[*]"
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val options: Options = parse_options(args)
    val fileList = mkdir(sparkSession, options.sourceF, options.targetF, options.ignoreFailures)
    //fileList.foreach(filepair => println("OUTPUT FILE ", filepair._1.toString(), filepair._2.toString()))
    cpfile(sparkSession, fileList, options.ignoreFailures, options.maxConcurrence)
  }

  def parse_options(args: Array[String]): Options = {
    var sourceF: String = "/Users/jingxiaozhang/Desktop/learn/Geek/spark_HW/source_folder"
    var targetF: String = "/Users/jingxiaozhang/Desktop/learn/Geek/spark_HW/target_folder"
    var maxConcurrence = 3
    var ignoreFailures = true
    args.sliding(2, 2).toList.collect {
      case Array("--source", value: String) => sourceF = value
      case Array("--target", value: String) => targetF = value
      case Array("-i", value: String) => ignoreFailures = value.toBoolean
      case Array("-m", value: String) => maxConcurrence = value.toInt
    }
    Options(sourceF, targetF, maxConcurrence, ignoreFailures)
  }

  def mkdir(sparkSession: SparkSession, source: String, target: String, ignoreFailures: Boolean): ArrayBuffer[(Path, Path)] = {
    val fileList: ArrayBuffer[(Path, Path)] = new ArrayBuffer[(Path, Path)]()
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    fs.listStatus(new Path(source)).foreach(currPath => {
      if (currPath.isDirectory) {
        val currBasename = currPath.getPath.toString().split(source)(1)
        val currTarget = new Path(target + currBasename)
        try {
          fs.mkdirs(currTarget)
        } catch {
          case exception: Exception => if (ignoreFailures) println("Copy directory exception: \n" + exception.getMessage) else throw exception
        }
        fileList.appendAll(mkdir(sparkSession, currPath.getPath.toString(), currTarget.toString(), ignoreFailures))
      } else {
        fileList.append((currPath.getPath, new Path(target)))
      }
    })
    fileList
  }

  def cpfile(sparkSession: SparkSession, fileList: ArrayBuffer[(Path, Path)], ignoreFailures: Boolean, numConcur: Int): Unit = {
    val concurRDD: RDD[(Path, Path)] = sparkSession.sparkContext.parallelize(fileList, numConcur)
    concurRDD.foreachPartition(concurIter => {
      val conf = new Configuration()
      concurIter.foreach(filepair => {
        try {
          FileUtil.copy(filepair._1.getFileSystem(conf), filepair._1, filepair._2.getFileSystem(conf), filepair._2, false, conf)
        } catch {
          case exception: Exception => if (ignoreFailures) println("Copy directory exception: \n" + exception.getMessage) else throw exception
        }
      })
    })
  }
}
