package Spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.concurrent.ExecutionContext.global
import scala.xml.dtd.ContentModelParser.acc

object Accumulator {


def main(args: Array[String]) {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("DropDuplicates")
    .getOrCreate()
 val sc = spark.sparkContext

  val readdf = spark.read.option("inferSchema", true).option("header",true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\emp_date.csv")

    readdf.filter(col("DOJ")==="1624886742").count()

  var acc = spark.sparkContext.accumulator(0)

  readdf.foreach(_ => acc.add(1))
  println(s"Total number of rows processed: ${acc.value}")



}}