package Spark

import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml.functions.from_xml
import com.databricks.spark.xml.{from_xml_string, schema_of_xml}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object ReadXML {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("ReadXML")
      .getOrCreate()

 val readxml = spark.read.option("header",true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\Mydata.csv")


   readxml.show(false)
   // import spark.implicits._

  // val myschema = schema_of_xml(readxml.select("worklog").as[String])

  // val payload = readxml.withColumn("payload",from_xml($"worklog",myschema))
//    println("Inferred Schema:")
//    myschema.printTreeString()
   // payload.show()
  }
}
