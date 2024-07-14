package Spark

import org.apache.spark.sql.SparkSession

object remove {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DropDuplicates")
      .getOrCreate()

    val to_remove = List("%", "-", "&", ":")
    val remove_from = List("XYZ", "12%", "MALE", "ABC", "1-7KG", "12Kg", "Male&Female", "1:1", "script")

    val ans = remove_from.filter(p => !to_remove.exists(x => p.contains(x)))

    ans.foreach(println)

  }
}