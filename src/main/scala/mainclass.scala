import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
trait mainclass extends Serializable {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("MySpark")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

  }
}
