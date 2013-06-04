package DBCommunication;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;


public class MailStoringWorkerStarter {
	private static Properties props=new Properties();
	private static int minWorkerCount;
	private static int maxWorkerCount;
	private static double messagesPerWorker;
	private static double messagesPerWorkerThreshold;
	private static String queueUrl;
	private static long waitingTime;
	private static long idleWaitingTime;
	private static DbConnectorFactory dbf;
	public static String debug;
	public static ArrayList<MailStoringWorker> workerList=new ArrayList<MailStoringWorker>();
	private static QueueSizeCheckerThread queueChecker;
	private static long checkIntervalMillis;
	
	public static void main(String[] args){
		
		//read properties
    	String propertiesPath;
    	
    	if(args.length==0){
    		//default properties file
    		propertiesPath="MailApp.properties";
    	}else{
    		//only one argument supported (properties file)
	    	propertiesPath=args[0];
    	}		
			
			try{
				props.load(new FileInputStream(propertiesPath));
			}
			catch (IOException e){
				System.out.println(e.getMessage());
				System.exit(0);
			}
		dbf = new DbConnectorFactory(props);
		minWorkerCount=Integer.valueOf(props.getProperty("queueworker.minworkercount","2"));
		maxWorkerCount=Integer.valueOf(props.getProperty("queueworker.maxworkercount","10"));
		messagesPerWorker=Double.valueOf(props.getProperty("queueworker.messagesperworker","20"));
		messagesPerWorkerThreshold=Double.valueOf(props.getProperty("queueworker.messagesperworkerthreshold","10"));
		checkIntervalMillis=Long.valueOf(props.getProperty("queueworker.checkqueuesizeinterval","20000"));
		queueUrl=props.getProperty("mailQueueUrl","https://sqs.us-east-1.amazonaws.com/940855283934/test_Queue_1");
		waitingTime=Long.valueOf(props.getProperty("queueworker.waitingtime","50"));
		idleWaitingTime=Long.valueOf(props.getProperty("queueworker.idlewaitingtime","5000"));
		String awsPropertiesFile = props.getProperty("awspropertiesfile","AwsCredentials.properties");
		debug=props.getProperty("queueworker.debug","false");
				
		
		//Start min# Workers
		try {
			for(int i = 0; i<minWorkerCount;i++){
			startMailStoringWorker(queueUrl, waitingTime, idleWaitingTime, awsPropertiesFile,workerList );
			
			}
		} catch (MailAppDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		queueChecker = new QueueSizeCheckerThread(queueUrl, awsPropertiesFile, checkIntervalMillis,workerList,minWorkerCount,maxWorkerCount,messagesPerWorker,messagesPerWorkerThreshold);
		queueChecker.start();
	}
	public static synchronized MailStoringWorker startMailStoringWorker(String queueUrl, long waitingTime, long idleWaitingTime, String awsPropertiesFile, ArrayList<MailStoringWorker> workerList) throws MailAppDBException{
		
		MailStoringWorker msw = new MailStoringWorker(dbf.createDBConnectorInstance(), queueUrl, waitingTime, idleWaitingTime, awsPropertiesFile, debug);
		workerList.add(msw);
		msw.start();
		return msw;
	}

	public static void stopMailStoringWorker( ArrayList<MailStoringWorker> workerList, MailStoringWorker msw) {
		
		workerList.remove(msw);
		msw.stopMailstoringWorker();
		
	}
	public static ArrayList<MailStoringWorker> getWorkerList(){
		return workerList;
	}
	public static long getWaitingTime(){
		return waitingTime;
	}
	public static long getIdleWaitingTime(){
		return idleWaitingTime;
	}

}

