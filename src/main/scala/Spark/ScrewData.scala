package Spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ScrewData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("screwdata")
      .getOrCreate()


  val df = spark.read.option("header",true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\Revenue_live.csv")

    df.show(false)
    df.count()

   val part = df.repartition(col("GST_State_Code"))

   val partid = part.withColumn("partition_id",spark_partition_id())
     .groupBy("partition_id").count().show()





  }
}

