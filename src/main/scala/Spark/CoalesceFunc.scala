package Spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object CoalesceFunc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DropDuplicates")
      .getOrCreate()

 val readdf = spark.read.option("header", true)csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\PlayersData")

    readdf.createTempView("read")

    val df2 = spark.sql("select Player,coalesce(Score1,Score2,Score3,Score4) as Scores from readsdf")

    df2.show()
    readdf.filter(col("Score1") isNull).show


  }
  }
