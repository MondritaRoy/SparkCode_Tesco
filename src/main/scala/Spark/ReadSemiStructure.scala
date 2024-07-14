package Spark

import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ReadSemiStructure {

  def main(args:Array[String]) {

    var spark = SparkSession.builder()
      .master("local[*]")
      .appName("SemiStructure")
      .getOrCreate()



    val MySchema = new StructType()
      .add("TimeStamp",StringType,nullable = true)
      .add("pid",StringType,nullable = true)
      .add("price",StringType,nullable = true)
      .add("ViewTime",StringType,nullable = true)

    val readdf = spark.read.schema(MySchema).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\Logs.csv")

   // readdf.show(false)

    val df1 = readdf.withColumn("DateTimestamp", rtrim(split(readdf("TimeStamp")," ")(0)))
      .withColumn("Channel",rtrim(split(readdf("TimeStamp")," ")(1)))
      .withColumn("Event",rtrim(split(readdf("TimeStamp")," ")(3))).drop("TimeStamp")
df1.show(false)

/*
    val df2 = df1.withColumn("PID",rtrim(split(df1("pid"),"=")(1)))
      .withColumn("Event",rtrim(split(df1("Event"),"=")(1)))
      .withColumn("price",rtrim(split(df1("price"),"=")(1)))
      .withColumn("ViewTime",rtrim(split(df1("ViewTime"),"=")(1)))

    df2.show(false)*/

    var cols = List("pid","price","ViewTime","Event")

    cols.foldLeft(df1){(newdf, colname) =>
      newdf.withColumn(colname,rtrim(split(df1(colname),"=")(1)))}.show(false)
  }

}
