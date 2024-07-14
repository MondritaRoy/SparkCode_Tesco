package Spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object DateFunction {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Datefunc")
      .getOrCreate()


    var schem = StructType(Seq(StructField("Customer_No", IntegerType, true),
      StructField("Card_type", StringType, true),
      StructField("Date", DateType, true),
      StructField("Category", StringType, true),
      StructField("Transaction Type", StringType, true),
      StructField("Amount", DoubleType, true)
    ))


    val df = spark.read.option("header", true).option("inferSchema", true).option("DateFormat", "M/d/yyyy").schema(schem).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\personal_transactions.csv")

    // df.show(10,false)
    // df.printSchema()


    val epochdf = spark.read.option("header", true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\Date_Epoch.csv")

    //    epochdf.show(10, false)
    //    epochdf.printSchema()

    epochdf.withColumn("date", to_date(from_unixtime(col("Epoch"))))


    val datedf = spark.read.option("header", true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\DateMotivation.csv")

    datedf.show()

    val newdf = Array("hiredate", "report_date", "end_date")

    import spark.implicits._
//
//    newdf.foldLeft(datedf){(abc,colnames) =>
//      abc.withColumn(colnames, when(to_date(datedf(colnames),"dd-MMM-yyyy").isNotNull, to_date(datedf(colnames),"dd-MMM-yyyy"))
//          .when(to_date(datedf(colnames),"dd/MM/yy").isNotNull, to_date(datedf(colnames),"dd-MMM-yyyy")))


    }


}