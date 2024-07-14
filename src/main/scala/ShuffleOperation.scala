import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object ShuffleOperation {


  def main(args: Array[String]): Unit = {

    val spark = new SparkConf()
      .setAppName("dynamic_header")
      .setMaster("local")


    val sc = new SparkContext(spark)

    val rdd = sc.parallelize(Array(("a", 4), ("a", 4), ("b", 4), ("c", 1)))
  //AggregrateByKey
    val rddagg = rdd.aggregateByKey(0)(_ + _, _ + _)
   // rddagg.foreach(println)
//CombineBykey

    val combines = rdd.combineByKey((x:Int) => x,
    (acc:Int, x:Int) => acc + x,
      (acc1:Int, acc2:Int) => acc1 + acc2,
      new HashPartitioner(1)).foreach(println)

   val txt = sc.textFile("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\abc.txt")
   val newrdds = txt.flatMap(x=>x.split(" ")).map(x => (x,1)).reduceByKey(_+_)
    newrdds.foreach(println)
    newrdds.sortByKey(ascending = true).foreach(println)
  }
}