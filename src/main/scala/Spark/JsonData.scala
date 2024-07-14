package Spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object JsonData {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("WriteDf")
      .getOrCreate()


    val df = spark.read.option("header",true).option("multiline",true).option("escape","\"").csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\dummy2.csv")

//      df.show(false)
//  df.printSchema()
    import spark.implicits._

  val df2 =   df.withColumn("Response",json_tuple(col("request"),"Response")).drop("request")

   // df2.show

    df2.select(
        json_tuple(col("Response"), "MessageId").alias("MessageId")).show


    val myjson= spark.read.json("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\input1.json")

//    myjson.show()

    val df3 = spark.read.json("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\Jsonfile.json")
    df3.select(col("e_id")).show(false)
    val explodeas = df3.select(col("e_id"))


    df3.withColumn("eid", explode(col("e_id"))).show()
  }

  }
