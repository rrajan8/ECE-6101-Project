package jobs;

import common.*;

//Hello World
public class Hello extends Job {

  @Override
  public void config() {
    setNumTasks(2); //set the number of tasks
  }

  @Override
  public void task(int tId) {
    try{
      Thread.sleep(1000);
      System.out.println("task"+tId+": Hello World"); //this string will be printed out from worker instead of client
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
}
