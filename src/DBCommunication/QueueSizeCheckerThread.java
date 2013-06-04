package DBCommunication;

import java.util.ArrayList;
import java.util.Map;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;

public class QueueSizeCheckerThread extends Thread {
	private String queueURL;
	private AmazonSQS sqs;
	private long checkInterval;
	private int latestQueueSize=0;

	
	private int minWorkers;
	private int maxWorkers;
	private double messageCountPerWorker;
	private double messageCountPerWorkerTolerance;
	private String awsPropertiesFile;
	public QueueSizeCheckerThread(String queueURL, String awsPropertiesFile, long checkInterval, ArrayList<MailStoringWorker> workerList, int minWorkers, int maxWorkers, double messageCountPerWorker, double messageCountPerWorkerTolerance){
		this.checkInterval=checkInterval;
		this.queueURL=queueURL;
		this.awsPropertiesFile=awsPropertiesFile;
		sqs=new AmazonSQSClient(new ClasspathPropertiesFileCredentialsProvider(awsPropertiesFile));
		this.messageCountPerWorker=messageCountPerWorker;
		this.messageCountPerWorkerTolerance=messageCountPerWorkerTolerance;

		this.minWorkers=minWorkers;
		this.maxWorkers=maxWorkers;
	}
	
	public void run(){
while(true){
		int currentWorkerCount = MailStoringWorkerStarter.workerList.size();
		
		
			latestQueueSize=getApproximateQueueSize();
			double currentMessagesPerWorker = latestQueueSize/currentWorkerCount;
			double difference = messageCountPerWorker-currentMessagesPerWorker;
			printDebug("current msg/worker: "+currentMessagesPerWorker+" difference to "+messageCountPerWorker+" : "+difference+" Threshold: "+messageCountPerWorkerTolerance);
			if(Math.abs(difference)>messageCountPerWorkerTolerance){
				if(Math.signum(difference)==-1.0){
					long additionalWorkers = Math.round(Math.abs(difference)/messageCountPerWorker);
					if(additionalWorkers+currentWorkerCount>maxWorkers){
						additionalWorkers=maxWorkers-currentWorkerCount;
					}
					printDebug("adding "+additionalWorkers+" Workers, QueueSize: "+latestQueueSize+" total Workers: "+(currentWorkerCount+additionalWorkers));
					for(long i = 0; i<additionalWorkers;i++){
								try {									
									MailStoringWorkerStarter.startMailStoringWorker(queueURL, MailStoringWorkerStarter.getWaitingTime(), MailStoringWorkerStarter.getIdleWaitingTime(), awsPropertiesFile, MailStoringWorkerStarter.workerList);
								
								} catch (MailAppDBException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
						}
				}else{
					double targetWorkerCount = latestQueueSize / messageCountPerWorker;
					
					long lessWorkers = Math.round(currentWorkerCount-targetWorkerCount);
					
					if(currentWorkerCount-lessWorkers<minWorkers){
						lessWorkers=currentWorkerCount-minWorkers;
					}
					
					 printDebug("removing "+lessWorkers+" Workers, QueueSize: "+latestQueueSize+" total Workers: "+(currentWorkerCount-lessWorkers));
					
					for(long i = 0; i<lessWorkers;i++){
						MailStoringWorker msw = MailStoringWorkerStarter.workerList.get(0);
						MailStoringWorkerStarter.stopMailStoringWorker(MailStoringWorkerStarter.workerList,msw);
						
					}
				}
			
		}
		
		try {
			sleep(checkInterval);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	}
	
	public int getApproximateQueueSize(){
		String attrib = "ApproximateNumberOfMessages";
		GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest().withQueueUrl(queueURL).withAttributeNames(attrib);
		Map<String,String> result = sqs.getQueueAttributes(getQueueAttributesRequest).getAttributes();
		return Integer.parseInt(result.get(attrib));
	}
	public void printDebug(String s){
		if(MailStoringWorkerStarter.debug.equalsIgnoreCase("true")){
			System.out.println("DEBUG: QueueSizeChecker: "+s);
		}
	}

}
