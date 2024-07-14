import org.apache.spark.sql.SparkSession

import scala.io.Source

object DynamicHeader {

  def main(args:Array[String]): Unit = {

  val spark =SparkSession
    .builder()
    .master("local[*]")
    .appName("dynamic_header")
    .getOrCreate()

  val lines = Source.fromFile("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\headerforsampledata").getLines()
  val columns = lines.flatMap( x=> x.split(",")).toSeq

  val df = spark.read.csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\SampleDataWithoutHeader")
    val columndf = df.toDF(columns:_*)

    columndf.show()
  }

}
