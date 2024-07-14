package Spark

import org.apache.spark.sql.SparkSession

object Repartition {
  def main(args: Array[String]): Unit = {

    var spark = SparkSession.builder()
      .master("local[*]")
      .appName("Datefunc")
      .getOrCreate()


    val df =spark.read.option("header",true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\empdata")


  //  df.select("EmpId").distinct().show(false)

println(df.repartition(3).rdd.getNumPartitions)

    val df2 =spark.read.option("header",true).csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\mask_data.csv")

    println(mask_data("Dona@uhduh"))

  def mask_data(colval:String) :Unit =
  {

    val string = colval.split("@")(0).toCharArray

    var a = ""
    for(i <- 0 to string.length -1){

      if(i==0 | i == string.length-1){
        a = a + string(i)
 }
 else {
   a = a+"*"
 }
      return a+"@"+colval.split("@")(1)

    }
  }
  df2.createOrReplaceTempView("Temp")
    df2.show()

  spark.sql("select substring(split(email,'@')[0],1,1) || regexp_replace(substring(split(email,'@')[0],2,length(split(email,'@')[0])-1),'[A-Za-z0-9_.]','*')  from Temp").show(false)

  }

}