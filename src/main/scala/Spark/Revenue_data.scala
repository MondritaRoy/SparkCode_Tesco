package Spark

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object Revenue_data {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Revenue_data")
      .getOrCreate()

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark_project.jetty").setLevel(Level.ERROR)

 val df = spark.read.option("header",true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\Revenue_live.csv")

   val rowCount =  df.count()
    println(s"Number of rows in DataFrame: $rowCount")
  val df2 =  df.select("Projected_Unit")


   // df2.write.mode("overwrite").option("header",true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\project")
   // df.printSchema()
  }
}