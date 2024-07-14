package Spark

import org.apache.ivy.plugins.trigger.Trigger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object StructuredStream {
  def main(args: Array[String]) :Unit={
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DropDuplicates")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")
    spark.conf.set("spark.sql.shuffle.partition",3)

    val df = spark.read
      .text("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\SampleData.txt")

    df.show
val replace_col= df.withColumn("chk",regexp_replace(col("value"),"(.*?\\|){5}", "$0-" ))

    //"((?:[^|]+\\|){5})([^|]+)", "$1-|$2"
    //"(.*?\\|){5}"

    replace_col.show(false)

val explode_df = replace_col.withColumn("col_explode",explode(split(col("chk"),"\\|-")))

 val col_exp =    explode_df.select(split(col("col_explode"), "\\|"))

    col_exp.show(false)

    val column = Array("Name","Edu","Yearofexp","Tech","Mob")

    val resultDF = col_exp.select(
      (0 until 5).map(i => col("""split(col_explode, \|)""").getItem(i).as(column(i))): _*
    )
    resultDF.show(false)
  }
}