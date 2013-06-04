package MailAppUtils;

import java.util.Date;
import java.util.Random;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class QueueFiller {

	/**
	 * @param args
	 */
	
	public static String[] vornamen = {"Hans", "Joerg", "Martin", "Gunther", "Linda", "Anne", "Pete", "Jane"};
	public static String[] nachnamen = {"Maier", "Becker", "Schmitt", "Müller", "Huber"};
	public static String[] domains = {"@gmail.com","@gmx.de","@web.de"};
	public static String messagebody ="Hallo,\r\nTestmessage\r\nTestmessage body";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		storeMessageInQueue(3);
		
		
		
	}
	
	
	public static void storeMessageInQueue(int messageNumber) {
		System.out.println("Storing Message...");

		// Send a message
		AmazonSQS sqs = new AmazonSQSClient(
				new ClasspathPropertiesFileCredentialsProvider());
		String myQueueUrl = sqs.listQueues().getQueueUrls().get(0);
		

		
		try {
		
			for(int i = 0; i<=messageNumber;i++){

		
			sqs.sendMessage(new SendMessageRequest(myQueueUrl, buildMessage()));
			
			}
		} catch (AmazonServiceException ase) {
			System.out
					.println("Caught an AmazonServiceException, which means your request made it "
							+ "to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out
					.println("Caught an AmazonClientException, which means the client encountered "
							+ "a serious internal problem while trying to communicate with SQS, such as not "
							+ "being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}

		System.out.println("Message stored to " + myQueueUrl);

		// connect to db ( Cassandra ) and store message
		// OR write Message to SQS to be consumed by Storage Worker ->
		// better? more scalable, db independent
	}
	public static String buildMessage(){
		int vorname1 = new Random().nextInt(vornamen.length-1);
		int nachname1 = new Random().nextInt(nachnamen.length-1);
		int vorname2 = new Random().nextInt(vornamen.length-1);
		int nachname2 = new Random().nextInt(nachnamen.length-1);
		int domain1 = new Random().nextInt(domains.length-1);
		int domain2 = new Random().nextInt(domains.length-1);
		String msg = vornamen[vorname1]+nachnamen[nachname1]+domains[domain1]+"\r\n"+vornamen[vorname2]+nachnamen[nachname2]+domains[domain2]+"\r\n"+messagebody+"\r\n"+new Date().toString();
		return msg;
	}

}
