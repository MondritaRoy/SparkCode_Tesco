package Spark

import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DelimeterCleaning {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DelimeterClean")
      .getOrCreate()

    //val df: DataFrame = spark.read.option("header",true).option("delimeter","~|").csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\DelimeterCleaning")


    val df: DataFrame = spark.read.text("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\DelimeterCleaning")

    val head = df.first()

val schema = head.mkString("~|").split("~|")


import spark.implicits._

    val withoutheaderss = df.filter(x => x!= head).rdd.map(x=> x.getString(0).split("~\\|")).toDF("schemaa")
    withoutheaderss.show()

    val withoutheader = df.filter(x => x!= head).toDF("txt")

    withoutheader.withColumn("split_data",split(col("txt"),"~\\|"))
      .select(col("split_data").getItem(0).as("Name"), col("split_data").getItem(1).as("Age"))
      .show(false)


  }
}