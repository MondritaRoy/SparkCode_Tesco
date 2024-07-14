package Spark

import org.apache.spark.sql.SparkSession

object HiveOperationThroughSpark {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("HivethroughSpark")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()




  }

}
