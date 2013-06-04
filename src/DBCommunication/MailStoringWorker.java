package DBCommunication;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import MailAppUtils.MailAppMessage;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

public class MailStoringWorker extends Thread{

/* Usecases: 
 * 
 * store Mail in Box (read SQS->[retrieve big mails from s3]->store message in DB 
 * (sending User's outbox, receiving User's inbox)->delete Messages from Queue[ and s3])
 * 
 * , Retrieve mails from box (+scan)
	*/

	//db used
public DbConnector db;

//public AmazonS3 s3 = null; //only initialize s3 client if needed
private String awsPropertiesFile;
public AmazonSQS sqs;
public String queueUrl;

private boolean stop=false;
long sleeptime; //timespan to wait between quering sqs for new messages
long extendedSleeptime;//additional timespan to wait if no message was received in the last request
public List<Message> sqsMessagesList;
public Message[] sqsMessagesArray;
public int maxMessagecount;

public int maxNumberOfMessages =10;

private String debug;


/*
//read from table
	
//write to table

//udpate item
	
//delete record/item from table
	
//scan table
*/

public void run() {
	
	while(!stop){
	
		try{
		if(sqsMessagesList==null||sqsMessagesList.size()<=0){
				sqsMessagesList = getQueueMessage(queueUrl);
				
				if(sqsMessagesList.size()<=0){
					//wait if no message was received in the last request
					sleep(extendedSleeptime);
				}
		}else{
			int listSize = sqsMessagesList.size();
			printDebug("Processing "+listSize+" Messages");
			
			for(int i =0;i<listSize;i++){
				
				Message sqsMessage = sqsMessagesList.get(i);
				if(sqsMessage!=null){
					MailAppMessage mailMsg = new MailAppMessage(sqsMessage.getBody(), sqsMessage.getAttributes() );
					try{
					db.storeMessage(mailMsg);

					//System.out.println("removing message from List...");
					//sqsMessagesList.remove(sqsMessage);
					sqsMessagesList.set(i, null);
					//System.out.println("...removed");
					deleteMessageFromQueue(queueUrl, sqsMessage.getReceiptHandle());
					
					}catch (MailAppDBException mae) {
						//MailStoringWorkerStarter.stopMailStoringWorker(MailStoringWorkerStarter.getWorkerList(), this);
						//stop=true;
						mae.printStackTrace();
						
					}
				}
				
			}
			sqsMessagesList = null;
		}
		
		sleep(sleeptime);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	printDebug("...stopped.");
   
}

public void stopMailstoringWorker(){
	printDebug("Stopping MailStoringWorkerThread... ");
	stop=true;
}


		
	//get sqs messages	
	public List<Message> getQueueMessage(String queueUrl){
		// Receive messages
       
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        //request sent timestamp along with message
        Vector<String> requestAttributes = new Vector<String>();
        requestAttributes.add("SentTimestamp");
        
        receiveMessageRequest.setAttributeNames(requestAttributes);
        receiveMessageRequest.setMaxNumberOfMessages(maxNumberOfMessages);
        printDebug("(id: "+this.getId()+")receiving Messages from queue "+queueUrl);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        return messages;
       
	}
/*	
	//retrieve s3 message	
	public String getMessagefromS3(String bucketName, String key){
		s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider());
		
		System.out.println("Downloading an object");
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
        System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
        try {
        	return getTextfromInputStream(object.getObjectContent());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "ERROR";
		}
  	

        
	}   
	
						    private  String getTextfromInputStream(InputStream input) throws IOException {
						        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
						        String inputStreamText="";
						        while (true) {
						            String line = reader.readLine();
						            if (line == null) break;
						            inputStreamText+=line+"\r\n";
						            //System.out.println("    " + line);
						        }
						       
						        return inputStreamText;
						    }
		*/   
	

	//delete consumed sqs messages
	public  void deleteMessageFromQueue(String queueUrl, String messageRecieptHandle){
		 // Delete a message
       // System.out.println("Deleting a message from smtp queue...");
        sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageRecieptHandle));	
        //System.out.println("...deleted");
	}
	
	/*
	//delete s3 message	
	public void deleteMessagefromS3(String bucketName, String s3key){
		s3.deleteObject(bucketName, s3key);
	}
	*/
	
	public MailStoringWorker(DbConnector db, String queueUrl,long waitingTime, long idleWaitTime, String awsPropertiesFile, String debug){
		this.debug=debug;
		this.db = db;
		this.sleeptime = waitingTime;
		this.extendedSleeptime = idleWaitTime;
		this.awsPropertiesFile = awsPropertiesFile;
		sqs=new AmazonSQSClient(new ClasspathPropertiesFileCredentialsProvider(awsPropertiesFile));
		try {
			db.init();
		} catch (MailAppDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.queueUrl = queueUrl;
	}
	private void printDebug(String s){
		if(debug.equalsIgnoreCase("true"))
		 System.out.println("DEBUG MailStoringWorker ID "+this.getId()+": "+s);
	}

}
