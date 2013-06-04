package SMTPServer;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Properties;
import java.util.UUID;

import javax.naming.directory.DirContext;

import org.subethamail.smtp.MessageContext;
import org.subethamail.smtp.MessageHandler;
import org.subethamail.smtp.MessageHandlerFactory;
import org.subethamail.smtp.RejectException;

import DBCommunication.DbConnector;
import DBCommunication.DbConnectorFactory;
import DBCommunication.MailAppDBException;
import MailAppUtils.MailAppMessage;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class MyMessageHandlerFactory implements MessageHandlerFactory {

	public long maxMessageSize;
	public String mailQueueUrl;
	public Properties props;
	private String debug;
	String awsPropertiesFile;
	AmazonSQS sqs;
	private static DbConnectorFactory dbf;

	public boolean driectDbConnection = false;
	
    public MyMessageHandlerFactory(Properties p) {
		this.props=p;
		debug=p.getProperty("smtpserver.debug","false");
		this.maxMessageSize = Long.valueOf(p.getProperty("smtpserver.maxMessageSize","63000"));
		awsPropertiesFile = props.getProperty("awspropertiesfile","AwsCredentials.properties");
		this.driectDbConnection = Boolean.parseBoolean(props.getProperty("smtpserver.directdbconnection","true"));
		if(!driectDbConnection){
			this.mailQueueUrl = p.getProperty("mailQueueUrl",null);
			sqs = new AmazonSQSClient(new ClasspathPropertiesFileCredentialsProvider(awsPropertiesFile));
		}else{
			dbf = new DbConnectorFactory(props);
			
		}
		
	}

	public MessageHandler create(MessageContext ctx) {
        return new Handler(ctx);
    }

    class Handler implements MessageHandler {
        MessageContext ctx;
        
        String frm = "";
        String rcpt = "";
        String message ="";

        public Handler(MessageContext ctx) {
                this.ctx = ctx;
        }
        
        public void from(String from) throws RejectException {
                //System.out.println("FROM:"+from);
                frm=from;
        }

        public void recipient(String recipient) throws RejectException {
               // System.out.println("RECIPIENT:"+recipient);
                rcpt = recipient;
        }

        public void data(InputStream data) throws IOException {
        	String dataString = this.convertStreamToString(data);
        	
                
                message = frm+"\n"+rcpt+"\n"+dataString;
                if(	(message.getBytes().length<maxMessageSize)){ 
               
                storeMessageInQueue(message);
                }else{
                	//System.out.println("message too big - store in s3 and send link to queue");
                	printDebug("Message too big. Message is not stored - too big (>"+maxMessageSize);
                	//storeMessageInS3(message);
                }
        }

        public void done() {
                //System.out.println("Finished");
        }

        public String convertStreamToString(InputStream is) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                StringBuilder sb = new StringBuilder();
                
                String line = null;
                try {
                        while ((line = reader.readLine()) != null) {
                                sb.append(line + "\n");
                        }
                } catch (IOException e) {
                        e.printStackTrace();
                }
                return sb.toString();
        }
        
		public void storeMessageInQueue(String message) {
			//System.out.println("Storing Message...");

			// Send a message

			if(driectDbConnection){
				DbConnector db=null;
	        	try {
					db=dbf.createDBConnectorInstance();
				} catch (MailAppDBException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				MailAppMessage mailMsg = new MailAppMessage(message, ""+System.currentTimeMillis()+"_"+UUID.randomUUID() );
				try{
				//printDebug("Calling DB-Instance to store message "+mailMsg.getTimeuuid()+" length: "+mailMsg.getSize()+"\n"+mailMsg.getWholeMessageWITHOUT2LINES());
				db.storeMessage(mailMsg);
				
				}catch (MailAppDBException mae) {
					//MailStoringWorkerStarter.stopMailStoringWorker(MailStoringWorkerStarter.getWorkerList(), this);
					//stop=true;
					mae.printStackTrace();
					
				}
				
			}else{
			
				try {
					sqs.sendMessage(new SendMessageRequest(mailQueueUrl, message));
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
	
				printDebug("Message stored to " + mailQueueUrl);
	
				// connect to db ( Cassandra ) and store message
				// OR write Message to SQS to be consumed by Storage Worker ->
				// better? more scalable, db independent
			
			}
			
		}
		public void storeMessageInS3(String message){
			AmazonS3 s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider());
			
	        String bucketName = "mailAppTestBucket_1";
	        String key = "message_"+UUID.randomUUID();
			//System.out.println("Uploading a new object to S3 from a file\n");
            try {
				s3.putObject(new PutObjectRequest(bucketName, key, createMessageFile(message) ));
			} catch (AmazonServiceException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (AmazonClientException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
		}
		
		private File createMessageFile(String message) throws IOException {
	        File file = File.createTempFile("message", ".txt");
	        file.deleteOnExit();

	        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
	        writer.write(message);
	        writer.close();

	        return file;
	    }
		private void printDebug(String s){
			if(debug.equalsIgnoreCase("true")){
				System.out.println("SMTP Server: "+s);
			}
		}

    }
}