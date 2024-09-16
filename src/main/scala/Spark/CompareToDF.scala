package Spark

import org.apache.spark.sql.SparkSession

object CompareToDF extends App{

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("DropDuplicates")
    .getOrCreate()


val df1 =  spark.read.option("header", true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\CustomerDF1")

val df2 =   spark.read.option("header", true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\CustomerDF2")

  //One way but it gives only dataset from 1 dataframe
  df1.except(df2).show()
 //Another Way to get all the data difference from both the table
  df1.createTempView("table1")
  df2.createTempView("table2")

 val getdatafromdf1 = spark.sql("select * from table1 minus select * from table2")

 val getdatafromdf2 =   spark.sql("select * from table2 minus select * from table1")

 println( getdatafromdf1.unionAll(getdatafromdf2).count)


}
