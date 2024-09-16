package Spark

import org.apache.spark.sql.SparkSession

object CompareToDF extends App{

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("DropDuplicates")
    .getOrCreate()


val df1 =  spark.read.option("header", true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\CustomerDF1")

val df2 =   spark.read.option("header", true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\CustomerDF2")

  df1.except(df2).show()

}
