class ThreadExample extends Thread{


  override def run() {

  print("Thread is running")

  }
}

object mainobj{


def main(args:Array[String]): Unit = {

  var x = new ThreadExample

  x.start()

}
}