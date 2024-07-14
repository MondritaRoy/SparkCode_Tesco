package Spark

import org.apache.spark.sql.SparkSession

object DropDuplicates {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DropDuplicates")
      .getOrCreate()


    val df = spark.read.option("header", true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\DuplicateData")

    df.select("Name","Age").distinct().show()
    df.dropDuplicates("Name","Age").show()
  }
}