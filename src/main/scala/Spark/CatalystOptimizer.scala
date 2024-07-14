package Spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object CatalystOptimizer {

   def main(args:Array[String]): Unit = {

     val spark = SparkSession.builder()
       .master("local[*]")
       .appName("Catalyst")
       .getOrCreate()

     val orderdf = spark.read.csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\order_items.csv")
     .toDF("order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price")
     orderdf.show(false)


     val categoriesdf = spark.read.csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\categoroes.csv")
     .toDF("category_id","category_department_id","category_name")

     categoriesdf.show(false)

     val productsdf = spark.read.csv("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\products.csv")
     .toDF("product_id","product_category_id","product_name","product_description","product_price","product_image")

     productsdf.show(false)

//     val resdf = productsdf.select("product_id","product_category_id","product_name")
//       .join(categoriesdf.select("category_id","category_department_id","category_name"),
//         col("product_category_id") === col("category_id"))
//       .join(orderdf.select("order_item_id","order_item_product_id","order_item_subtotal"),
//         col("order_item_id")===col("product_category_id"))
//       .filter(col("category_name") === "Accessories")
//       .groupBy("category_name","product_name")
//       .agg(round(sum(col("order_item_subtotal")),2)).as("revenue")
//         .withColumn("rank", rank() over Window.partitionBy("category_name").orderBy("revenue"))
//       .orderBy(desc("revenue"))

     val rdd1 = spark.sparkContext.textFile("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\products.csv")

     val rdd2 = spark.sparkContext.textFile("C:\\Users\\Admin\\IdeaProjects\\Spark_Project\\src\\main\\resources\\categoroes.csv")

     val pair_rdd1 = rdd1.map(x => x.split(",")).map(x => (x(1),(x(0),x(1),x(2),x(3),x(4),x(5))))
     pair_rdd1.foreach(println)

     val pair_rdd2 = rdd2.map(x => x.split(",")).map(x => (x(0),(x(0),x(1),x(2))))

     //pair_rdd1.join(pair_rdd2).foreach(println)


   }

}
