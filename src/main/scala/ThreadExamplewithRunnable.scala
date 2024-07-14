class ThreadExamplewithRunnable extends Runnable{


  override def run() {

println("Thread is running")

  }
}


object mainobject{

  def main(args:Array[String]): Unit = {

    val x = new ThreadExamplewithRunnable
    val y = new Thread(x)
    y.start()

  }


}