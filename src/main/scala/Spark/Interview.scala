package Spark

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Interview {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ArrayExplodeAndPivot")
      .master("local")
      .getOrCreate()

    import spark.implicits._


    val data = Seq(
      ("c1", Seq(1, 2, 4)),
      ("c2", Seq(3, 4, 5))
    ).toDF("id", "value")


    val resultDF = data
      .withColumn("A", $"value".getItem(0))
      .withColumn("B", $"value".getItem(1))
      .withColumn("C", $"value".getItem(2))
      .drop("value")



    var sc=  spark.sparkContext


    val df =   sc.textFile("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\abc.txt")

    val readdf = spark.read.csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\empdata")

    val header = readdf.first()

    val withoutheader = readdf.filter(x => x!=header).show(false)

   val x = "my name is dona roy chakraborty"


  val df2 =   df.flatMap(x => x.split(" ")).map(word => (word,1)).reduceByKey(_ + _)


   // df2.collect()

    val numbers = sc.parallelize(List(1, 2, 3, 4))
    val alphabets = sc.parallelize(List("a", "b", "c", "d"))
    val zippedPairs = numbers.zip(alphabets)
    //zippedPairs.foreach(println)

 //adding 2 dataframe having different no of column structure how will you merge rowbasis

   val data1 =Seq((1, "manish", "data engineer"), (2, "rani", "data analyst"), (3, "manju", "data science"))

   val column1 =List("id", "name", "department")

    val schema = StructType(Seq(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("department", StringType, true)
    ))


    val data2 = Seq((3, "harish"), (4, "monish"), (5, "priti"))

    val column2 = List("id", "name")

    val schema1 = StructType(Seq(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)))

    val df3 = spark.read.option("mode","FAILFAST").csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\abc.txt")


    df3.show()



    val df5 = spark.createDataFrame(sc.parallelize(data1).map(row => Row.fromTuple(row)),schema)



    val df6 = spark.createDataFrame(sc.parallelize(data2).map(row => Row.fromTuple(row)),schema1)

    val df7 = df6.withColumn("dept", lit(""))
   // df5.union(df7).show()

    //how to convert table or view with the below comma seperated data.

    val datas = Array(("2344","35354","3232","3331"))

//
//    val df9 = spark.createDataFrame(datas).toDF("tuplecol")
//
//    val explodedf = df9.select(explode(col("tuplecol")))

    //explodedf.show()


  }

}