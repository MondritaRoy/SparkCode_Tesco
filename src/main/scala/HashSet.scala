import scala.collection.immutable.ListSet

object HashSet {

  def main(args: Array[String]) {
    var hashset = ListSet(4, 2, 8, 0, 6, 3, 45)
    hashset.foreach((element: Int) => println(element + " "))
    println(hashset) //. Elements are stored internally in reversed insertion order, 

  }
}
