
import scala.collection.immutable._
object ScalaSet {


  def main(args:Array[String]) {

    var newset = Set(3,4,5,6,8,"xtre")
    var sortset = SortedSet(1,3,45,6,78,8,9,9,0)
    var abhabet = Set(5,7,"Mon","tues","wded")
    var set = newset ++ abhabet
    var inter = abhabet.intersect(newset)
    var uni = newset.union(abhabet)
//    println(newset.head)
//    println(newset.tail)
//    println(set)
//    println(newset.contains(6))
//    println(inter)
//    println(uni)
    println(sortset)
  }

}
