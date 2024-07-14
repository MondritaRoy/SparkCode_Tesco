package Spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Pivot {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DropDuplicates")
      .getOrCreate()
    val data = Seq(
      (1, 85, "Math"),
      (1, 90, "Science"),
      (2, 75, "Math"),
      (2, 80, "Science"),
      (3, 95, "Math"),
      (3, 85, "Science")
    )

    val df = spark.createDataFrame(data).toDF("rollno", "marks", "subject")

    df.printSchema()

    df.groupBy("marks").pivot("subject").max("marks").show



  }
}