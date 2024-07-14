package Spark

import org.apache.spark.sql.SparkSession

object FindingthesmallestandLargest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("WriteDf")
      .getOrCreate()


    val wrd = "This is a test string"

    var newspt = wrd.split("\\s+")

    var minWord = ""
    var minLength = Int.MaxValue

    // Iterate through each word and update minWord and minLength if necessary
    newspt.foreach { word =>
      val length = word.length
      if (length < minLength) {
        minLength = length
        minWord = word
      }
    }


    println(s"Word with minimum length: $minWord, Length: $minLength")

  }

}