class ThreadSleepMethod extends Thread{

  override def run(){

   for(i <- 0 to 6){

    //print(i)
     println(this.getName()+" - "+i)
     Thread.sleep(500)
   }

  }

}


object mainobjs{

  def main(args:Array[String]): Unit = {

    var x = new ThreadSleepMethod
    var y = new ThreadSleepMethod
   // x.start()
    x.setName("First Name")
    //x.join() //The join() method wai
    // in() method is used to hold the execution of currently running thread until the specified thread finished it's execution.
  // y.start()
    y.setName("Second Thread")
    x.start()
    y.start()



  }




}