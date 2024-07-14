package Spark

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object salttt {

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SALTING")
      .getOrCreate()


    val df = spark.read.option("header", true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\Revenue_live.csv")

    df.show(false)

val newdf = "L2"

val df1 = df.withColumn("saltedcity",concat(col(newdf).cast("string"),(rand()*10).cast("int").cast("string")))

    df1.show(false)
  }
}
