package Spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFunction {


  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DropDuplicates")
      .getOrCreate()



    val readdf = spark.read.option("inferSchema", true).option("header",true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\emp_date.csv")

    val newdf = readdf.withColumn("DOJ", from_unixtime(col("DOJ")))

    newdf.withColumn("Year", year(col("DOJ")))
      .withColumn("Month", month(col("DOJ")))
      .withColumn("Dayofmonth", dayofmonth(col("DOJ")))
      .withColumn("Weekofyear", weekofyear(col("DOJ")))
      .withColumn("Quarter", quarter(col("DOJ")))
      .withColumn("LastDayOfMonth", last_day(col("DOJ")))
      .withColumn("addMonths", add_months(col("DOJ"),2))
      .withColumn("DateFormat", date_format(col("DOJ").cast("Date"),"MM-YYYY"))
      .show()


    readdf.createOrReplaceTempView("Tbl")

    spark.sql("select * from Tbl")




val newdfs =spark.read.option("header",true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\RechargeData")

    newdfs.show(1)
    import spark.implicits._

//

//val datedf = newdfs.select(to_date(col("rechargedate").cast("string"),"yyyyddMM")).show()
  //  val dfWithDate = newdfs.withColumn("dates",  date_format(to_date(col("rechargedate"), "yyyyMMdd"), "dd")).show()



  }
}
