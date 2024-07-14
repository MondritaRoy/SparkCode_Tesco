package Spark

import org.apache.hadoop.io.nativeio.NativeIO.Windows
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

object DuplicateRecord {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DropDuplicates")
      .getOrCreate()

val df = spark.read.option("header",true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\DuplicateData")

    df.groupBy("Name","Age","City").count().where("count > 1").drop("count").show

  val no = Window.partitionBy("Name").orderBy(col("City").desc)

    val df1 = df.withColumn("rank", row_number().over(no)).filter("rank > 1").drop("rank").dropDuplicates().show()


  }
}
