object Tuple {

  def main(args:Array[String]) {

    var tuple1 = (1,2,3,4)
    var tuple2 = (1,"India",4.0,7)
    var tuple3 = ("don","mon","son")
    print(tuple1)
    print(tuple2)
    print(tuple3)
    tuple1.productIterator.foreach(println)
    println(tuple2._1)
  }



}
