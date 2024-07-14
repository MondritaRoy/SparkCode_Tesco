package Spark

import org.apache.spark.sql.SparkSession

object SampleXml {

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DropDuplicates")
      .getOrCreate()

    val df = spark.read.format("xml")
  .option("rootTag", "catalog").option("rowTag", "book").load("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\Sample.xml")

df.show()
  }
}
