package Spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object RddsExample {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WorkingExample")

    val sc = new SparkContext(sparkConf)

    val readrdd = sc.textFile("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\abc.txt")


   val flatmap =  readrdd.flatMap(x => x.split(" "))
    val maps = flatmap.map(x => (x,1))
   val mapvalues=  maps.mapValues(x => x +1)


    var myrdd = Seq("a1", "b1", "c1", "s2", "s5")
    var newrdd = sc.parallelize(myrdd)

    newrdd.zipWithIndex().foreach(print)
    

  }
}