trait TraitExample {

def printss()
 print("repeat hello world")
}

class newtrait extends TraitExample {

  def printss(){
    println("sss")
  }
  def print(){

  println("Hello World")


}
}

object mains{

  def main (args:Array[String]): Unit = {


    val x = new newtrait
    x.print()
    x.printss()
  }

}

