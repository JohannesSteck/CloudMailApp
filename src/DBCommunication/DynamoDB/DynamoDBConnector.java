package DBCommunication.DynamoDB;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import DBCommunication.DbConnector;
import DBCommunication.MailAppDBException;
import MailAppUtils.CapacityLogger;
import MailAppUtils.MailAppMessage;
import MailAppUtils.StartSingleNodeMailApp;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.AttributeAction;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.DeleteItemRequest;
import com.amazonaws.services.dynamodb.model.DeleteItemResult;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.GetItemRequest;
import com.amazonaws.services.dynamodb.model.GetItemResult;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.ListTablesResult;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.amazonaws.services.dynamodb.model.ReturnValue;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.services.dynamodb.model.TableStatus;
import com.amazonaws.services.dynamodb.model.UpdateItemRequest;
import com.amazonaws.services.dynamodb.model.UpdateItemResult;

public class DynamoDBConnector extends DbConnector {

   
    private AmazonDynamoDBClient dynamoDB;
    private String awsPropertiesFile;
      

    
  //  public String usertable_tableName ="Usertable";
  //  public String usertable_hashKey ="user"; //username of registered users like pete, anne...
    public boolean createTables;
    
    public String mailboxtable_tableName;
    public long mailboxtable_readCapacity;
    public long mailboxtable_writeCapacity;
    public String mailboxtable_hashKey;	//username like pete, anne...
    public String mailboxtable_rangeKey;	//inobx, outbox...
    public String mailboxtable_mailcount;	//inobx, outbox...
    public String mailboxtable_mailboxsize;	//inobx, outbox...
    
    public String messagetable_tableName;
    public long messagetable_readCapacity;
    public long messagetable_writeCapacity;
    public String messagetable_hashKey ="mailboxname";	//name of the message (pete#inbox)
    public String messagetable_rangeKey ="timeuuid";	//timeuuid of the message
    public String messagetable_messageAttributeName;//the whole message itself
    //public String messagetable_messageSizeAttributeName;//the size of the message
    
    public String sizetable_tableName;
    public long sizetable_readCapacity;
    public long sizetable_writeCapacity;
    public String sizetable_hashKey ="mailboxname";	//name of the message (pete#inbox)
    public String sizetable_rangeKey ="timeuuid";	//timeuuid of the message
    public String sizetable_sizeAttributeName;//the whole message itself


   
    
    public String keyDelim;//Delimiter for Message table's Mailboxname (like pete<delimiter>inbox)
    
    public int maxMessagecount;
    
    public Properties props;
    public String debug;
    public boolean logCapacity;
    public int logCapacityInterval;

   /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public DynamoDBConnector(Properties p){
    	this.props = p;
    }
    
    public void init() throws MailAppDBException {
    	//get Properties
    	createTables=Boolean.parseBoolean(props.getProperty("dbproperties.dynamodb.createTables","flase"));
    	
    	mailboxtable_tableName=props.getProperty("dbproperties.dynamodb.mailboxtable_tableName","Mailboxes");
    	mailboxtable_hashKey=props.getProperty("dbproperties.dynamodb.mailboxtable_hashKey","username");
    	mailboxtable_rangeKey=props.getProperty("dbproperties.dynamodb.mailboxtable_rangeKey","mailbox");
    	mailboxtable_mailcount=props.getProperty("dbproperties.dynamodb.mailboxtable_mailcount", "mailcount");
    	mailboxtable_mailboxsize=props.getProperty("dbproperties.dynamodb.mailboxtable_mailboxsize", "mailboxsize");
    	mailboxtable_readCapacity=Long.valueOf(props.getProperty("dbproperties.dynamodb.mailboxtable_readCapacity","3"));
    	mailboxtable_writeCapacity=Long.valueOf(props.getProperty("dbproperties.dynamodb.mailboxtable_writeCapacity","1"));
    	
    	messagetable_tableName=props.getProperty("dbproperties.dynamodb.messagetable_tableName","Messages");
    	messagetable_hashKey=props.getProperty("dbproperties.dynamodb.messagetable_hashKey","mailboxname");
    	messagetable_rangeKey=props.getProperty("dbproperties.dynamodb.messagetable_rangeKey","timeuuid");
    	messagetable_messageAttributeName=props.getProperty("dbproperties.dynamodb.messagetable_messageAttributeName","message");
    	//messagetable_messageSizeAttributeName=props.getProperty("dbproperties.dynamodb.messagetable_messageSizeAttributeName","size");
    	messagetable_readCapacity=Long.valueOf(props.getProperty("dbproperties.dynamodb.messagetable_readCapacity","3"));
    	messagetable_writeCapacity=Long.valueOf(props.getProperty("dbproperties.dynamodb.messagetable_writeCapacity","2"));
    	
    	sizetable_tableName=props.getProperty("dbproperties.dynamodb.sizetable_tableName","Sizetable");
    	sizetable_hashKey=props.getProperty("dbproperties.dynamodb.sizetable_hashKey","mailboxname");
    	sizetable_rangeKey=props.getProperty("dbproperties.dynamodb.sizetable_rangeKey","timeuuid");
    	sizetable_sizeAttributeName=props.getProperty("dbproperties.dynamodb.sizetable_sizeAttributeName","size");
    	//messagetable_messageSizeAttributeName=props.getProperty("dbproperties.dynamodb.messagetable_messageSizeAttributeName","size");
    	sizetable_readCapacity=Long.valueOf(props.getProperty("dbproperties.dynamodb.sizetable_readCapacity","3"));
    	sizetable_writeCapacity=Long.valueOf(props.getProperty("dbproperties.dynamodb.sizetable_writeCapacity","2"));
    	
    	keyDelim=props.getProperty("dbproperties.dynamodb.keyDelim","#");
    	
    	maxMessagecount=Integer.valueOf(props.getProperty("maxmessagecount","100"));
    	
    	debug = props.getProperty("dbproperties.dynamodb.debug","false");
    	logCapacity = Boolean.parseBoolean(props.getProperty("dbproperties.dynamodb.logcapacity","false"));
    	

    	
        // initialize DynamoDb driver & table.
    	try {
			createClient();		
			if(createTables){
				//create Table if not existing
				if(!tableExists(mailboxtable_tableName)){
					createHashAndRangeKeyTable(mailboxtable_tableName, mailboxtable_hashKey, "S", mailboxtable_rangeKey, "S", mailboxtable_readCapacity, mailboxtable_writeCapacity);
					waitForTableToBecomeAvailable(mailboxtable_tableName);
				}
				if(!tableExists(messagetable_tableName)){
					createHashAndRangeKeyTable(messagetable_tableName, messagetable_hashKey, "S", messagetable_rangeKey, "S", messagetable_readCapacity, messagetable_writeCapacity);
					waitForTableToBecomeAvailable(messagetable_tableName);
	
				}
				if(!tableExists(sizetable_tableName)){
					createHashAndRangeKeyTable(sizetable_tableName, sizetable_hashKey, "S", sizetable_rangeKey, "S", sizetable_readCapacity, sizetable_writeCapacity);
					waitForTableToBecomeAvailable(sizetable_tableName);
	
				}
			}
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Error while working with DynamoDB:");
			e.printStackTrace();
		}


    }
    public boolean tableExists(String tablename){
    	ListTablesResult ltr = dynamoDB.listTables();
    	
    	if(ltr.getTableNames().contains(tablename)){
    		return true;
    	}else{
    		return false;
    	}
    }
    
    public void createHashKeyTable(String tableName, String keyAttributeName, String keyAttributeType, long readCapacity, long writeCapacity){

    	CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
        .withKeySchema(new KeySchema(new KeySchemaElement().withAttributeName(keyAttributeName).withAttributeType(keyAttributeType)))
        .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(readCapacity).withWriteCapacityUnits(writeCapacity));
    TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
    System.out.println("Created Table: " + createdTableDescription);


    }
    public void createHashAndRangeKeyTable(String tableName, String hashKeyAttributeName, String hashKeyAttributeType,String rangeKeyAttributeName, String rangeKeyAttributeType, long readCapacity, long writeCapacity){

    	KeySchema ks = new KeySchema();
    	ks.withHashKeyElement(new KeySchemaElement().withAttributeName(hashKeyAttributeName).withAttributeType(hashKeyAttributeType))
    	.withRangeKeyElement(new KeySchemaElement().withAttributeName(rangeKeyAttributeName).withAttributeType(rangeKeyAttributeType));
    	
    	CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
        .withKeySchema(ks)
        .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(readCapacity).withWriteCapacityUnits(writeCapacity));
    TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
    System.out.println("Created Table: " + createdTableDescription);

    
    }
    private void waitForTableToBecomeAvailable(String tableName) {
        System.out.println("Waiting for " + tableName + " to become ACTIVE...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {Thread.sleep(1000 * 20);} catch (Exception e) {}
            try {
                DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
                TableDescription tableDescription = dynamoDB.describeTable(request).getTable();
                String tableStatus = tableDescription.getTableStatus();
                System.out.println(" - current state: " + tableStatus);
                if (tableStatus.equals(TableStatus.ACTIVE.toString())) return;
            } catch (AmazonServiceException ase) {
                if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException") == false) throw ase;
            }
        }

        throw new RuntimeException("Table " + tableName + " never went active");
    }
    
    public void storeMessage(MailAppMessage message) throws MailAppDBException {
        // initialize DynamoDb driver & table.
    	
    	//check if user is registered (usertable)
    	//check if mailbox exists (mailboxtable)
    	//store message in sender's outbox and receiver's inbox
    	String from = message.getFrom();
    	String to = message.getTo();
    	String outboxName = "outbox";
    	String inboxName = "inbox";
    	
    	//check if mailboxes are full. if yes, don't store in that mailbox.
    	
    	int outboxCount=getMessageCount(message.getFrom(), "outbox");
		if(outboxCount<maxMessagecount){
    	storeMessageInMailbox(message, from, outboxName);
		}else{
			printDebug("outbox of user "+message.getFrom()+" full");
		}
		
		int inboxCount=getMessageCount(message.getTo(), "inbox");
		if(inboxCount<maxMessagecount){
    	storeMessageInMailbox(message, to, inboxName);
		}else{
			printDebug("inbox of user "+message.getTo()+" full");
		}


    }
    
    public void createClient() throws Exception {
    	awsPropertiesFile = props.getProperty("awspropertiesfile","AwsCredentials.properties");
    	dynamoDB = new AmazonDynamoDBClient(new ClasspathPropertiesFileCredentialsProvider(awsPropertiesFile));
    }
    //not used
    /*
    public void addUser(String username){
    	Map<String, AttributeValue> users = new HashMap<String, AttributeValue>();
    	users.put(usertable_hashKey, new AttributeValue().withS(username));
    	PutItemRequest request = new PutItemRequest().withTableName(usertable_tableName).withItem(users);
        dynamoDB.putItem(request);
    }
    //not used
    public void addMailbox(String username, String mailboxName){
    	 Map<String, AttributeValue> mailbox = new HashMap<String, AttributeValue>();  
         mailbox.put(mailboxtable_hashKey, new AttributeValue().withS(username));
         mailbox.put(mailboxtable_rangeKey, new AttributeValue().withS(mailboxName));
    	PutItemRequest request = new PutItemRequest().withTableName(mailboxtable_tableName).withItem(mailbox);
        dynamoDB.putItem(request);
    }
    */
    public void storeMessageInMailbox(MailAppMessage message, String username, String mailboxName) throws MailAppDBException {

            Map<String, AttributeValue> msg = new HashMap<String, AttributeValue>();
            
            //store pete#inbox as mailboxname (hash key)
            msg.put(messagetable_hashKey, new AttributeValue().withS(username+keyDelim+mailboxName));
            //store a certain timeuuid of the message as timeuuid (range key)
            msg.put(messagetable_rangeKey, new AttributeValue().withS(message.getTimeuuid()));
            //store the message itself under the message attribute
            msg.put(messagetable_messageAttributeName, new AttributeValue().withS(message.getWholeMessage()));
            PutItemRequest messagetable_request = new PutItemRequest().withTableName(messagetable_tableName).withItem(msg);
            //printDebug("trying to store: "+messagetable_request);
            
            Map<String, AttributeValue> sizetable_msg = new HashMap<String, AttributeValue>();
            
            //store pete#inbox as mailboxname (hash key)
            sizetable_msg.put(sizetable_hashKey, new AttributeValue().withS(username+keyDelim+mailboxName));
            //store a certain timeuuid of the message as timeuuid (range key)
            sizetable_msg.put(sizetable_rangeKey, new AttributeValue().withS(message.getTimeuuid()));

            //store the size under the size attribute in sizetable
            sizetable_msg.put(sizetable_sizeAttributeName, new AttributeValue().withS(""+message.getSize()));
            PutItemRequest sizetable_request = new PutItemRequest().withTableName(sizetable_tableName).withItem(sizetable_msg);
            //printDebug("trying to store: "+sizetable_request);

            
            try{
            PutItemResult pur1 = dynamoDB.putItem(messagetable_request);
            
            
            printDebug("storeMessageInMailbox-messagetable: "+pur1.getConsumedCapacityUnits()+" size: "+message.getSize());
            PutItemResult pur2 = dynamoDB.putItem(sizetable_request);  
            printDebug("storeMessageInMailbox-sizetable: capa: "+pur2.getConsumedCapacityUnits()+" size: "+message.getSize());
            if(logCapacity){
            	StartSingleNodeMailApp.messagetable_caplogger.logWriteCapacity(pur1.getConsumedCapacityUnits());
            	StartSingleNodeMailApp.sizetable_caplogger.logWriteCapacity(pur2.getConsumedCapacityUnits());
            }
            
            increaseMailcountAndSize(username, mailboxName, message.getSize());
            
            }catch(Exception e){
            	e.printStackTrace();
            	throw new MailAppDBException();
            }
      
    }
	public void increaseMailcountAndSize(String user, String mailbox, int mailsizeInByte) {
		Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
		updateItems.put(mailboxtable_mailcount, 
				  new AttributeValueUpdate()
				    .withAction(AttributeAction.ADD)
				    .withValue(new AttributeValue().withN("+1")));
		updateItems.put(mailboxtable_mailboxsize, 
				  new AttributeValueUpdate()
				    .withAction(AttributeAction.ADD)
				    .withValue(new AttributeValue().withN("+"+mailsizeInByte)));
		
		Key key = new Key().withHashKeyElement(new AttributeValue().withS(user)).withRangeKeyElement(new AttributeValue().withS(mailbox));
		
		UpdateItemRequest updateItemRequest = new UpdateItemRequest()
		  .withTableName(mailboxtable_tableName)
		  .withKey(key).withReturnValues(ReturnValue.UPDATED_NEW)
		  .withAttributeUpdates(updateItems);
		
		
		            
		UpdateItemResult result = dynamoDB.updateItem(updateItemRequest);
		if(logCapacity){        
			StartSingleNodeMailApp.mailboxtable_caplogger.logWriteCapacity(result.getConsumedCapacityUnits());
        }
		printDebug("increaseMailcountAndSize: update Result: "+result.toString());
	}
	public void decreaseMailcountAndSize(String user, String mailbox, int mailsizeInByte, int numberToDecrease) {
		Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
		updateItems.put(mailboxtable_mailcount, 
				  new AttributeValueUpdate()
				    .withAction(AttributeAction.ADD)
				    .withValue(new AttributeValue().withN("-"+numberToDecrease)));
		updateItems.put(mailboxtable_mailboxsize, 
				  new AttributeValueUpdate()
				    .withAction(AttributeAction.ADD)
				    .withValue(new AttributeValue().withN("-"+mailsizeInByte)));
		
		Key key = new Key().withHashKeyElement(new AttributeValue().withS(user)).withRangeKeyElement(new AttributeValue().withS(mailbox));
		
		UpdateItemRequest updateItemRequest = new UpdateItemRequest()
		  .withTableName(mailboxtable_tableName)
		  .withKey(key).withReturnValues(ReturnValue.UPDATED_NEW)
		  .withAttributeUpdates(updateItems);
		
		
		            
		UpdateItemResult result = dynamoDB.updateItem(updateItemRequest);
		if(logCapacity){        	
			StartSingleNodeMailApp.mailboxtable_caplogger.logWriteCapacity(result.getConsumedCapacityUnits());
        }
		printDebug("decreaseMailcountAndSize: update Result: "+result.toString());
	}
	
	public void correctMailcount(String user, String mailbox) throws MailAppDBException {
		int count = getRealMessageCount(user, mailbox);
		
		Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
		updateItems.put(mailboxtable_mailcount, 
				  new AttributeValueUpdate()
				    .withAction(AttributeAction.PUT)
				    .withValue(new AttributeValue().withN(Integer.toString(count))));
		
		
		Key key = new Key().withHashKeyElement(new AttributeValue().withS(user)).withRangeKeyElement(new AttributeValue().withS(mailbox));
		
		UpdateItemRequest updateItemRequest = new UpdateItemRequest()
		  .withTableName(mailboxtable_tableName)
		  .withKey(key).withReturnValues(ReturnValue.UPDATED_NEW)
		  .withAttributeUpdates(updateItems);
		
		
		            
		UpdateItemResult result = dynamoDB.updateItem(updateItemRequest);
		//printDebug("correctMailcount: update Result: "+result.toString());
	}

	@Override
	public boolean validateUser(String user, String pass) throws MailAppDBException {
		//TODO no authentication up to now
		return true;
		
	}

	@Override
	public String pop3stat(String user, String mailbox)throws MailAppDBException {
		
		GetItemRequest getItemRequest = new GetItemRequest()
        .withTableName(mailboxtable_tableName)
        .withKey(new Key()
            .withHashKeyElement(new AttributeValue().withS(user))
            .withRangeKeyElement(new AttributeValue().withS(mailbox)))
        .withAttributesToGet(Arrays.asList(mailboxtable_mailcount, mailboxtable_mailboxsize));
    
    GetItemResult result = 
    	dynamoDB.getItem(getItemRequest);
    
    if(logCapacity){    	
    	StartSingleNodeMailApp.mailboxtable_caplogger.logReadCapacity(result.getConsumedCapacityUnits());
    }
    
    printDebug("pop3stat u: "+user+" readCapa: "+result.getConsumedCapacityUnits());
    
    String s="0 0";
    if(result!=null){	   
    	if(result.getItem()!=null){
	    	s = result
	    	.getItem()
	    	.get(mailboxtable_mailcount)
	    	.getN()+" "
	    	+result
	    	.getItem()
	    	.get(mailboxtable_mailboxsize)
	    	.getN();
    	}
	   }
    //returns a String for pop3 use like "3 2142" (3 Mails with Size(characters of Message TODO calculate octets))
    return s;
	}

	
	@Override
	public int getMessageCount(String user, String mailbox)throws MailAppDBException {
		//retrieves Message count value from counter table
		return Integer.valueOf(pop3stat(user, mailbox).split(" ")[0]);
		
	}
	public int getRealMessageCount(String user, String mailbox)throws MailAppDBException {
		//really counts all Messages
		QueryRequest queryRequest = new QueryRequest()
		.withTableName(sizetable_tableName)
		.withCount(true)
		.withHashKeyValue(new AttributeValue(user+keyDelim+mailbox));
		
		QueryResult qr = dynamoDB.query(queryRequest);
		return qr.getCount();
		
	}

	@Override
	public void deleteMessages(String user, String mailbox,
			ArrayList<MailAppMessage> deleteList) throws MailAppDBException {

		int totalSize = 0;
		int itemsDeleted = 0;
		

		
		for (MailAppMessage msg : deleteList){

			try{
				//delete from messagetable
			DeleteItemRequest deleteItemRequest  = new DeleteItemRequest()
			.withTableName(messagetable_tableName)
			.withKey(
					new Key()
					.withHashKeyElement(new AttributeValue().withS(user+keyDelim+mailbox))
					.withRangeKeyElement(new AttributeValue().withS(msg.getTimeuuid())));
			
			DeleteItemResult result = dynamoDB.deleteItem(deleteItemRequest);
			printDebug("Deleting item from "+messagetable_tableName+" with "+msg.getTimeuuid()+" msg Size: "+msg.getSize()+" writeCapa: "+result.getConsumedCapacityUnits());
			
			//delete from sizetable
			DeleteItemRequest sizetable_deleteItemRequest  = new DeleteItemRequest()
			.withTableName(sizetable_tableName)
			.withKey(
					new Key()
					.withHashKeyElement(new AttributeValue().withS(user+keyDelim+mailbox))
					.withRangeKeyElement(new AttributeValue().withS(msg.getTimeuuid())));
			
			DeleteItemResult sizetable_result = dynamoDB.deleteItem(sizetable_deleteItemRequest);
			printDebug("Deleting item from "+sizetable_tableName+" with "+msg.getTimeuuid()+" writeCapa: "+sizetable_result.getConsumedCapacityUnits());
			
			if(logCapacity){
				StartSingleNodeMailApp.messagetable_caplogger.logWriteCapacity(result.getConsumedCapacityUnits());
				StartSingleNodeMailApp.sizetable_caplogger.logWriteCapacity(sizetable_result.getConsumedCapacityUnits());
	        }

			totalSize = totalSize + msg.getSize();
			itemsDeleted = itemsDeleted + 1;
			
			
			
			
			
			}catch (AmazonServiceException ase) {
				System.err.println("Failed to delete item in " + messagetable_tableName);
			}
			
			
			
		}
		decreaseMailcountAndSize(user, mailbox, totalSize, deleteList.size());
		
		
	}
	private void printDebug(String s){
		if(debug.equals("true")){
			System.out.println("DynamoDBConnector: "+s);
		}
	}

	@Override
	public MailAppMessage getMessageByID(String user, String mailbox,
			String timeUUID) throws MailAppDBException {

		Key key = new Key(new AttributeValue(user+keyDelim+mailbox),new AttributeValue(timeUUID));
		
		GetItemRequest getItemRequest = new GetItemRequest();
		getItemRequest.withTableName(messagetable_tableName).withKey(key).withAttributesToGet(messagetable_messageAttributeName);

		GetItemResult result = dynamoDB.getItem(getItemRequest);
		String retrievedMessage = result.getItem().get(messagetable_messageAttributeName).getS();
		
		if(logCapacity){
			StartSingleNodeMailApp.messagetable_caplogger.logReadCapacity(result.getConsumedCapacityUnits());
        }
		
		printDebug("getMessageByID u: "+user+keyDelim+mailbox+" key: "+timeUUID+" readCapa: "+result.getConsumedCapacityUnits()+ " size: "+retrievedMessage.length());
		
		return new MailAppMessage(retrievedMessage,timeUUID);

	}

	@Override
	public ArrayList<MailAppMessage> getMessagesUIdList(String user,
			String mailbox) throws MailAppDBException {
		
		ArrayList<MailAppMessage> resultArrayList = new ArrayList<MailAppMessage>();
		
		Key lastKeyEvaluated = null;
		
		do {
			
		
		QueryRequest queryRequest = new QueryRequest()
		.withTableName(sizetable_tableName)
		.withHashKeyValue(new AttributeValue(user+keyDelim+mailbox))
		.withAttributesToGet(sizetable_rangeKey, sizetable_sizeAttributeName)
		.withExclusiveStartKey(lastKeyEvaluated);
		
		
		QueryResult qr = dynamoDB.query(queryRequest);
		printDebug("getMessageUIdList: u: "+user+keyDelim+mailbox+" readCap: "+qr.getConsumedCapacityUnits());
		//System.out.println(qr);

		if(logCapacity){
			StartSingleNodeMailApp.sizetable_caplogger.logReadCapacity(qr.getConsumedCapacityUnits());
        }
		
		//TODO handle max Items retrieved
		

		for(int i = 0; i< qr.getCount();i++){
			String timeuuid = qr.getItems().get(i).get(sizetable_rangeKey).getS();
			long size = Long.valueOf(qr.getItems().get(i).get(sizetable_sizeAttributeName).getS());
			resultArrayList.add(new MailAppMessage(timeuuid, size));
		}
		
		 lastKeyEvaluated = qr.getLastEvaluatedKey();
		 
		 
		
	} while (lastKeyEvaluated != null);
		
		return resultArrayList;
	}

    

}
