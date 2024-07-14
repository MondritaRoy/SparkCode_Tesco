import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods._
import scala.io.Source
object sparkintitation {

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("READJSON")
      //.enableHiveSupport()
      .getOrCreate()

    PropertyConfigurator.configure(getClass.getClassLoader.getResource("log4j.properties"))
    val df1 = spark.read.option("badRecordsPath","C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\badrecord" ).format("json").load("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources")


    val df2 = spark.read.option("mode","dropMalFormed").format("json").load("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources")


    val df3 = spark.read.option("mode","FAILFAST").format("json").load("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources")


    val rdd = spark.sparkContext.textFile("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\badrecord.json")



    val jsonrdd = rdd.map{x => val jsonrow = parse(x)
      (compact(jsonrow\ "state"),compact(jsonrow\ "Capital"))}

    jsonrdd.collect().foreach{println _}


  //  df1.repartition(3).write.partitionBy("state").parquet("dona")

  val firstpar =  rdd.mapPartitionsWithIndex((f:Int, itr :Iterator[String]) => { itr.toList.map(s => if (f == 0){
      println(s)}).iterator})

 //   println(firstpar.collect())

 val lines = Source

  }
}