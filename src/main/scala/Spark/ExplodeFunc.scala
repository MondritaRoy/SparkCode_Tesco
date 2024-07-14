package Spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object ExplodeFunc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DropDuplicates")
      .getOrCreate()

    val df = spark.read.option("header", true).option("delimiter", "|")
      .csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\ExplodeData")
    df.show()

    df.withColumn("Game" ,explode(split(df("Interests"),","))).drop("Interests").show()

    df.select(posexplode(split(df("Interests"),","))).show()
  }
}