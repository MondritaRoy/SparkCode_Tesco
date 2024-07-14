package Spark

import org.apache.spark.sql.SparkSession

object ReadingFiles {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("WriteDf")
      .getOrCreate()

    val df = spark.read.option("mergeSchema",true).parquet("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\userdata1.parquet")


   df.show()

    val df1 = df.filter(df("first_name").startsWith("A")).filter(df("gender")=== "Female")

   df.write.mode("overwrite").save("parquetdata")



  }
}
