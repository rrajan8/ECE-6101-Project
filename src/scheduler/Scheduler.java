package scheduler;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.LinkedList;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;





import common.*;

public class Scheduler {

  int schedulerPort;
  Cluster cluster;
  int jobIdNext;
  start_jobs executor;
	

  Scheduler(int p) {
    schedulerPort = p;
	executor = new start_jobs();
    cluster = new Cluster();
    executor.start();
    jobIdNext = 1;
  }
	
	

  public static void main(String[] args) {
    Scheduler scheduler = new Scheduler(Integer.parseInt(args[0]));
    scheduler.run();
  }
	
	public void run(){
		try{
			//create a ServerSocket listening at specified port
      ServerSocket serverSocket = new ServerSocket(schedulerPort);
			
			while(true){
				//create a new socket thread
				Socket socket = serverSocket.accept();
				DataInputStream dis = new DataInputStream(socket.getInputStream());
				DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
				
				int code = dis.readInt();
				if(code == Opcode.new_worker){
          //include the worker into the cluster
          WorkerNode n = cluster.createWorkerNode( dis.readUTF(), dis.readInt());
          if( n == null){
            dos.writeInt(Opcode.error);
          }
          else{
            dos.writeInt(Opcode.success);
            dos.writeInt(n.id);
            System.out.println("Worker "+n.id+" "+n.addr+" "+n.port+" created");
          }
          dos.flush();
		  dis.close();
          dos.close();
          socket.close();
        }

		if(code == Opcode.new_job){
			String className = dis.readUTF();
			long len = dis.readLong();

          //send out the jobId
			int jobId = jobIdNext++;
			dos.writeInt(jobId);
			dos.flush();

			//receive the job file and store it to the shared filesystem
			String fileName = new String("fs/."+jobId+".jar");
			FileOutputStream fos = new FileOutputStream(fileName);
			int count;
			byte[] buf = new byte[65536];
			while(len > 0) {
				count = dis.read(buf);
				if(count > 0){
				fos.write(buf, 0, count);
				len -= count;
				}
			}
			fos.flush();
			fos.close();

			//get the tasks
			int taskIdStart = 0;
			int numTasks = JobFactory.getJob(fileName, className).getNumTasks();
			job_request temp = new job_request(jobId, className, numTasks, socket);
			executor.push(temp);
			//dis.close();
			//dos.close();
		}

		if(code == Opcode.task_finish){
			int jobId = dis.readInt();
			int workId = dis.readInt();
			int taskId = dis.readInt();

			cluster.addFreeWorkerNode(cluster.workers.get(workId));
			executor.finished_tasks.add(new response(jobId, workId, taskId));

			dis.close();
			dos.close();

		}


				
			}
		}catch(Exception e) {
      e.printStackTrace();
    }
		
	}



//the data structure for a cluster of worker nodes
  class Cluster {
    public ArrayList<WorkerNode> workers; //all the workers
    LinkedList<WorkerNode> freeWorkers; //the free workers
    HeartbeatManager manager;
    
    Cluster() {
      workers = new ArrayList<WorkerNode>();
      freeWorkers = new LinkedList<WorkerNode>();
      manager = new HeartbeatManager();
      manager.start();
    }

    WorkerNode createWorkerNode(String addr, int port) {
      WorkerNode n = null;

      synchronized(workers) {
        n = new WorkerNode(workers.size(), addr, port);
        workers.add(n);
      }
      addFreeWorkerNode(n);

      synchronized(manager.checkin){
		Date timeNow = new Date();
		long timeInMilliseconds = timeNow.getTime();
      	manager.checkin.add(n.id, (Long)timeInMilliseconds);
      }

      return n;
    }

    WorkerNode getFreeWorkerNode() {
      WorkerNode n = null;

      try{
        synchronized(freeWorkers) {
          while(freeWorkers.size() == 0) {
            freeWorkers.wait();
          }
          n = freeWorkers.remove();
        }
        n.status = 2;
      } catch(Exception e) {
        e.printStackTrace();
      }

      return n;
    }

    void addFreeWorkerNode(WorkerNode n) {
      n.status = 1;
      synchronized(freeWorkers) {
        freeWorkers.add(n);
        freeWorkers.notifyAll();
      }
    }
  }

  //the data structure of a worker node
  class WorkerNode{
    int id;
    String addr;
    int port;
    int status; //WorkerNode status: 0-sleep, 1-free, 2-busy, 4-failed
	

    WorkerNode(int i, String a, int p) {
      id = i;
      addr = a;
      port = p;
      status = 0;
    }

		
  }
 
 class job_request{
		public int jobId;
		public String className;
		public String request_address;
		public int request_port;
		public int numTasks;
		public int taskIdStart;
		public int completed_tasks;
		public int outstandingTasks;
		Socket socket;
		job_request(int m_jobId, String m_className, int m_numTasks, Socket m_socket){
			jobId = m_jobId;
			className = m_className;
			numTasks = m_numTasks;
			socket = m_socket;
			taskIdStart = 0;
			outstandingTasks = 0;
			completed_tasks = 0;
		}
 }
	
	class start_jobs extends Thread{
		ArrayList<job_request> requests;
		public ArrayList<response> finished_tasks;


		public void push(job_request data){
    		requests.add(data);
 		}
		public job_request pop(int index){
    		return requests.remove(index);
  		}	
		
		start_jobs(){
			requests = new ArrayList<job_request>();
			finished_tasks = new ArrayList<response>();
		}
		
		public void run(){
			int count = 0;
			while(true){
				//update
				while(!finished_tasks.isEmpty()){
					response tempId = finished_tasks.remove(0);
					ListIterator<job_request> job = requests.listIterator();
					while(job.hasNext()){
						job_request tempr = job.next();
						if(tempr.jobId == tempId.jobId){
							tempr.outstandingTasks--;
							tempr.completed_tasks++;
							try{

								DataOutputStream dos = new DataOutputStream(tempr.socket.getOutputStream());
								dos.writeInt(Opcode.job_print);
								dos.writeUTF("task " + tempId.taskId + " finished on worker " + tempId.workId);
								dos.flush();
								//dos.close();
								job.set(tempr);
								
								if(tempr.completed_tasks == tempr.numTasks){
										dos.writeInt(Opcode.job_finish);
										dos.flush();
										dos.close();
										tempr.socket.close();
										job.remove();
									
								}
									

								
							}catch(Exception e){
								e.printStackTrace();
							}
						}
					}
				}

				
				WorkerNode node = cluster.getFreeWorkerNode();
			
				//schedule
				if(!requests.isEmpty()){
					count = count % requests.size();
					job_request temp = null;
					boolean found = false;
					
					while(count < requests.size() && !found ){
						temp = requests.get(count);
						if(temp.outstandingTasks < (temp.numTasks - temp.completed_tasks))
							found = true;
						else
							count++;
						
					}

					if(found && temp!=null){
						try{
							Socket workerSocket = new Socket(node.addr, node.port);
							DataOutputStream wos = new DataOutputStream(workerSocket.getOutputStream());

							wos.writeInt(Opcode.new_tasks);
							wos.writeInt(temp.jobId);
							wos.writeUTF(temp.className);
							wos.writeInt(temp.taskIdStart++);
							wos.flush();
							wos.close();
							workerSocket.close();
							temp.outstandingTasks++;
							requests.set(count,temp);
						}catch(Exception e){
							e.printStackTrace();
						}
					}
					else{
						cluster.addFreeWorkerNode(node);
					}

				}
				else{
					cluster.addFreeWorkerNode(node);
				}

				

			}
		}
		
		

	}

	class response{
		public int jobId;
		public int workId;
		public int taskId;
		response(int mjobId, int mworkId, int mtaskId){
			jobId = mjobId;
			workId = mworkId;
			taskId = mtaskId;
		}
	}

	final static int heartbeatPort = 4999; // Port used to listen for heartbeats
	
	class HeartbeatManager extends Thread {
		
		private Thread manager;
		ArrayList<Long>checkin; // list keeping track of the worker's status

		public HeartbeatManager(){
			System.out.println("Creating HeartbeatManager Thread");
			checkin = new ArrayList<Long>();
		}

		public void start(){
			System.out.println("Starting Heartbeat Manager" );
			if (manager == null)
			{
				manager = new Thread (this, "HeartbeatManager");
				manager.start ();
			}
	    }

	    public void run(){
		    try{
		    	ServerSocket heartbeatServer = new ServerSocket(heartbeatPort);
		    	int worker_id;
		    	long dateMS; // variable to store the date in milliseconds
		    	while(true){
			    	Socket socket = heartbeatServer.accept();
					DataInputStream dis = new DataInputStream(socket.getInputStream());


					// Update the list. Use times? 
					worker_id = dis.readInt();
			        synchronized(checkin){
				      	Date timeNow = new Date();
				      	long timeInMilliseconds = timeNow.getTime();
      					checkin.set(worker_id, timeInMilliseconds);
      				}
					// Should probably should create a thread to update the list to keep the server open to accept heartbeats
					System.out.println(new Date()+" : Heartbeat received from worker"+worker_id);

			    	// Close sockets and streams when finished
			    	dis.close();
			    	socket.close();
		    	}

		    }catch(Exception e) {
      			e.printStackTrace();
    		}

	    }

	    private class purgeDeadWorkers extends TimerTask{
		
		    public purgeDeadWorkers(){

		    }

			@Override
	      	public void run(){
	        	try{
	        		 synchronized(checkin){
					      	Date timeNow = new Date();
					      	long timeInMilliseconds = timeNow.getTime();
					      	for(int i = 0; i < checkin.size();i++){
					      		if((checkin.get(i) - timeInMilliseconds)>15000){
					      			System.out.println("Worker "+ i +" is inactive");
					      			checkin.set(i, null); 
					      		}
					      	}
	      					
	      				}
	        		
	        	}catch (Exception e){
	        		e.printStackTrace();
	        	}
	        }


		} 

	}


	// may not need this
	private class SocketLink{

		private int workerID;
		private int workerPort;
		private int managerPort;

		SocketLink(int workerPort, int managerPort){

		}
	}



}
