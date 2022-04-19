package spark.zjx

import java.io.File
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object invertedIndex {
  def main(args: Array[String]): Unit = {
    var appName = "invertedIndex"
    var master = "local[*]"
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    var inputpath = new File("/Users/jingxiaozhang/Desktop/learn/Geek/spark_HW/input/")
    val outputpath = "/Users/jingxiaozhang/Desktop/learn/Geek/spark_HW/output"
    val sc = new SparkContext(conf)
    val files = inputpath.listFiles.filter(_.isFile).map(_.getPath).toList
    var rdd_all = sc.emptyRDD[(String, String)]
    for(file <- files){
      val fname = file.split("/").last
      val frdd = sc.textFile(file).flatMap(_.split(" ").map((_, fname)))
      rdd_all = rdd_all.union(frdd)
    }
    val rdd2 = rdd_all.map(word => (word, 1)).reduceByKey(_ + _)
    val rdd3 = rdd2.map(word => (word._1._1, "("+word._1._2+","+word._2.toString+")")).reduceByKey(_ + "," + _).sortByKey()
    val rdd4 = rdd3.map(word => word._1+" : {"+word._2+"}")
    rdd4.repartition(1).saveAsTextFile(outputpath)
  }
}
