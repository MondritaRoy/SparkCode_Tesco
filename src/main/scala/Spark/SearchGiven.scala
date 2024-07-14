package Spark

import org.apache.spark.sql.SparkSession





object SearchGiven {

  def main(args:Array[String]): Unit = {

    var spark = SparkSession.builder()
      .master("local[*]")
      .appName("newjob")
      .getOrCreate()

var texts = "Given word is word so Check wisely"

 val sc = spark.sparkContext

 val newtext = sc.textFile(texts)
    println(newtext)
    print(newfind(texts))



  }

  def newfind(line: String): Unit = {

    line match {
      case s if s.contains("fff") =>
        println("Found the key")

      case _ => println(" NotFound the key")

    }
  }

}
