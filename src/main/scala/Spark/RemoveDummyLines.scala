package Spark

import org.apache.spark.sql.SparkSession
import  org.apache.spark.sql.functions._

object RemoveDummyLines {
def main(args: Array[String]): Unit = {

      var spark = SparkSession.builder()
        .master("local[*]")
        .appName("Datefunc")
        .getOrCreate()

      // val readdf = spark.read.csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\pageview.csv")


  var sc = spark.sparkContext

  var rdd1 =   sc.textFile("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\pageview.csv",2).map(x => x.split(","))

    rdd1.getNumPartitions
    rdd1.collect().mkString(",")

   val linestoskip = 8

    val filteredRDD =  rdd1.zipWithIndex().filter{
      case (_,index) => index >= linestoskip
    }.map(_._1)

    filteredRDD.collect()


}}