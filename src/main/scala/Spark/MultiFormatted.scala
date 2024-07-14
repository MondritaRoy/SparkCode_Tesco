package Spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, json_tuple}

object MultiFormatted {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DropDuplicates")
      .getOrCreate()

  val df = spark.read.option("escape","\"").option("header",true).option("multiline",true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\MultiformattedData")

  val newdf=  df.withColumn("jsondatafinal", json_tuple(col("JsonData"),"location"))

  newdf.withColumn("City",json_tuple(col("jsondatafinal"),"city"))
    .withColumn("State",json_tuple(col("jsondatafinal"),"state")).show

}
}