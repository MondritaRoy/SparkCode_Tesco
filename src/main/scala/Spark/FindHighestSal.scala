package Spark

import org.apache.spark.sql.SparkSession

object FindHighestSal {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("WriteDf")
      .getOrCreate()

  val df = spark.read.option("header",true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\empdata")

  df.createTempView("tables")

    val df2 = spark.sql("select Name, Salary,dense_rank() over(order by Salary desc) as rnk from tables group by Name,Salary")

    df2.filter( x => x(2) == 2).show()
  }

}
