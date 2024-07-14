package Spark

import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml
import com.databricks.spark.xml.util._
import org.apache.spark.sql.SaveMode

object
writeDataIntoXml {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("WriteDf")
      .getOrCreate()

   val df =  spark.read.option("header",true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\DuplicateData")

    df.show()

   //df.write.format("xml").option("rootTag","data").option("rowTag", "record").mode("Overwrite").save("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\writeData")



  }


}
