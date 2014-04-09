package worker;

import java.io.*;
import java.net.*;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import java.lang.*;

import common.*;

public class Worker {

  String schedulerAddr;
  int schedulerPort;
  String workerAddr;
  int workerPort;
  int heartbeatPort;

  public static void main(String[] args) {
    Worker worker = new Worker( args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
    worker.run();
  }

  Worker(String sAddr, int sPort, int wPort) {
    schedulerAddr = sAddr;
    schedulerPort = sPort;
    workerAddr = "localhost";
    workerPort = wPort;
    heartbeatPort = wPort+100;
  }

  public void run() {
    ServerSocket serverSocket;
    Socket socket, socketSchd;
    DataInputStream dis;
    DataOutputStream dos;
    int workerId;

    try{
      //create a ServerSocket listening at the specified port
      serverSocket = new ServerSocket(workerPort);

      //connect to scheduler
      socket = new Socket(schedulerAddr, schedulerPort);
      dis = new DataInputStream(socket.getInputStream());
      dos = new DataOutputStream(socket.getOutputStream());
   
      //report itself to the scheduler
      dos.writeInt(Opcode.new_worker);
      dos.writeUTF(workerAddr);
      dos.writeInt(workerPort);
      dos.flush();

      if(dis.readInt() == Opcode.error){
        throw new Exception("scheduler error");
      }
      workerId = dis.readInt();

      dis.close();
      dos.close();
      socket.close();

      //Create a heartbeat for the worker
      TimerTask heartbeat = new WorkerHeartbeat(workerId, 4999); //4999 is the heartbeat scheduler's port might pass that through the stream
      Timer timer = new Timer(true);
      timer.scheduleAtFixedRate(heartbeat, 0, 10 * 1000); // 10 sec * 1000 ms

      System.out.println("This is worker "+workerId);
      
      //repeatedly process scheduler's task assignment
      while(true) {
        //accept an connection from scheduler
        socket = serverSocket.accept();
        dis = new DataInputStream(socket.getInputStream());

        if(dis.readInt() != Opcode.new_tasks)
          throw new Exception("worker error");

        //get jobId, fileName, className, and the assigned task id
        int jobId = dis.readInt();
        String fileName = new String("fs/."+jobId+".jar");
        String className = dis.readUTF();
        int taskId = dis.readInt();
        dis.close();
        socket.close();


        //read the job file from shared file system
        Job job = JobFactory.getJob(fileName, className);
        job.task(taskId);
        /*
        //execute the assigned tasks
        for(int taskId=taskIdStart; taskId<taskIdStart+numTasks; taskId++){
          job.task(taskId);
          //report to scheduler once a task is finished
          dos.writeInt(Opcode.task_finish);
          dos.writeInt(taskId);
          dos.flush();
        }
        */
        //disconnect
        socketSchd = new Socket(schedulerAddr, schedulerPort);
        dos = new DataOutputStream(socketSchd.getOutputStream());
        dos.writeInt(Opcode.task_finish);
        dos.writeInt(jobId);
        dos.writeInt(workerId);
        dos.writeInt(taskId);
        dos.flush();
        dos.close();
        
      }
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  class WorkerHeartbeat extends TimerTask{
    
    private int workerID;
    private int workerPort;
    private int managerPort; //port that we'll send the heartbeat to

    public WorkerHeartbeat(int worker_id , int manager_port){
      System.out.println("Creating WorkerWatcher Thread");
      workerID = worker_id;
      workerPort = 4000+workerID; // Should probably find a cleaner way to do this later
      managerPort = manager_port;
    }

      @Override
      public void run(){
        try{
          //System.out.println("Sending heartbeat at:" + new Date());
          Socket workerSocket = new Socket(workerAddr, managerPort); // should actually be the host of the server, but since it's all localhost, it doesn't matter
          DataOutputStream wos = new DataOutputStream(workerSocket.getOutputStream());
          System.out.println("Worker"+ workerID + " is sending a heartbeat at "+new Date());
          wos.writeInt(workerID); // send worker ID 
          wos.close();
          //System.out.println("Done at:" + new Date());

      }catch(Exception e) {
          e.printStackTrace();
        }
      }

  }

}
