package DBCommunication.Cassandra;

import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Properties;

import me.prettyprint.cassandra.model.thrift.ThriftCounterColumnQuery;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;
import DBCommunication.DbConnector;
import DBCommunication.MailAppDBException;
import MailAppUtils.MailAppMessage;

public class CassandraDbConnector extends DbConnector{

	Properties props;
	Cluster cluster;
	String keyspaceName;
	Keyspace ksp;
	String mailboxTableName;
	String counterTableName;
	String sizeTableName;
	String counterColumnName;
	String counterMailboxSizeColumnName;
	String keyDelimiter;
	
	int maxMessagecount;

	String clusterNode;
	int replicationFactor;
	
	ColumnFamilyTemplate<String, String> templateMailboxes;
	ColumnFamilyTemplate<String, String> templateSizetable;
	ColumnFamilyTemplate<String, String> templateMessageCounter;

	String debug;
	
	public CassandraDbConnector(Properties p) {
		this.props=p;
		
	}

	@Override
	public void init() throws MailAppDBException {
		
		//get Properties
		keyspaceName=props.getProperty("dbproperties.cassandra.keyspaceName","MailAppKeyspace1");
		
    	mailboxTableName=props.getProperty("dbproperties.cassandra.mailboxTableName","Mailboxes");
    	counterTableName=props.getProperty("dbproperties.cassandra.counterTableName","Countertable");
    	sizeTableName=props.getProperty("dbproperties.cassandra.sizeTableName","Sizetable");
    	
    	counterColumnName=props.getProperty("dbproperties.cassandra.counterColumnName","messageCount");
    	counterMailboxSizeColumnName=props.getProperty("dbproperties.cassandra.counterMailboxSizeColumnName","mailboxSize");
    	clusterNode=props.getProperty("dbproperties.cassandra.clusterNode","localhost:9160");
    	replicationFactor=Integer.valueOf(props.getProperty("dbproperties.cassandra.replicationFactor","1"));
    	keyDelimiter=props.getProperty("dbproperties.cassandra.keyDelim","#");
    	
    	maxMessagecount=Integer.valueOf(props.getProperty("maxmessagecount","100"));
    	
    	debug=props.getProperty("dbproperties.cassandra.debug","false");
		
		cluster = HFactory.getOrCreateCluster("MailAppCluster",clusterNode);
		setUpSchema();
		
		
		ksp = HFactory.createKeyspace(keyspaceName, cluster);
		
		templateMailboxes = new ThriftColumnFamilyTemplate<String, String>(ksp,
            												mailboxTableName,
                                                           StringSerializer.get(),
                                                           StringSerializer.get());
		
		templateSizetable = new ThriftColumnFamilyTemplate<String, String>(ksp,
				sizeTableName,
               StringSerializer.get(),
               StringSerializer.get());
		
		templateMessageCounter = new ThriftColumnFamilyTemplate<String, String>(ksp,
				counterTableName,
                StringSerializer.get(),
                StringSerializer.get());
	}
	
	public void setUpSchema() {
		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keyspaceName);

		if (keyspaceDef == null) {
		
			ArrayList<ColumnFamilyDefinition> columnFamilyList = new ArrayList<ColumnFamilyDefinition>();
			
		ColumnFamilyDefinition cfDef_mailboxes = HFactory.createColumnFamilyDefinition(keyspaceName,
				mailboxTableName,
                ComparatorType.BYTESTYPE);
		
			columnFamilyList.add(cfDef_mailboxes);
			
		ColumnFamilyDefinition cfDef_sizetable = HFactory.createColumnFamilyDefinition(keyspaceName,
					sizeTableName,
	                ComparatorType.BYTESTYPE);
			
				columnFamilyList.add(cfDef_sizetable);
		
			/*
		
			ColumnFamilyDefinition cfDef_counter = HFactory.createColumnFamilyDefinition(keyspaceName,
					counterTableName,
					ComparatorType.COUNTERTYPE);
			cfDef_counter.setDefaultValidationClass(ComparatorType.COUNTERTYPE.getClassName());
        cfDef_counter.setColumnType(ColumnType.STANDARD);
		
        columnFamilyList.add(cfDef_counter);
        */
			
        ColumnFamilyDefinition cfDef_counter = HFactory.createColumnFamilyDefinition(keyspaceName, counterTableName);
        cfDef_counter.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
        cfDef_counter.setComparatorType(ComparatorType.UTF8TYPE);
        cfDef_counter.setDefaultValidationClass(ComparatorType.COUNTERTYPE.getClassName());
        cfDef_counter.setColumnType(ColumnType.STANDARD);
        
        columnFamilyList.add(cfDef_counter);

		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keyspaceName,
              ThriftKsDef.DEF_STRATEGY_CLASS,
              replicationFactor,
              columnFamilyList);
		
		cluster.addKeyspace(newKeyspace, true);
		System.out.println("keyspace added!");
		System.out.println(cluster.describeClusterName());
		}
	}
	
	@Override
	public void storeMessage(MailAppMessage message) throws MailAppDBException {
		
		//check mailbox sizes before storing. If too big don't store.
		int outboxCount=getMessageCount(message.getFrom(), "outbox");
		if(outboxCount<maxMessagecount){
		storeMessageInMailbox(message, message.getFrom(), "outbox");
		}else{
			printDebug("outbox of user "+message.getFrom()+" full");
		}
		
		int inboxCount=getMessageCount(message.getTo(), "inbox");
		if(inboxCount<maxMessagecount){
		storeMessageInMailbox(message, message.getTo(), "inbox");
		}else{
			printDebug("inbox of user "+message.getTo()+" full");
		}
	}
	 public void storeMessageInMailbox(MailAppMessage message, String username, String mailboxName) throws MailAppDBException {
		 try {
	            Mutator<String> mutator = HFactory.createMutator(ksp, StringSerializer.get());
	            //store whole Message

	            mutator.insert(username+keyDelimiter+mailboxName, mailboxTableName, HFactory.createStringColumn(message.getTimeuuid(), message.getWholeMessage()));
	            //store message size separately
	            mutator.insert(username+keyDelimiter+mailboxName, sizeTableName, HFactory.createStringColumn(message.getTimeuuid(), String.valueOf(message.getSize())));
	            //increase counters
	            mutator.incrementCounter(username+keyDelimiter+mailboxName, counterTableName, counterColumnName, 1);
	            mutator.incrementCounter(username+keyDelimiter+mailboxName, counterTableName, counterMailboxSizeColumnName, message.getSize());
	            /*
	            ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(ksp);
	            columnQuery.setColumnFamily(mailboxTableName).setKey(username+keyDelimiter+mailboxName).setName(message.getTimeuuid());
	            QueryResult<HColumn<String, String>> result = columnQuery.execute();
	            
	            System.out.println("Read HColumn from cassandra: " + result.get());            
	            System.out.println("Verify on CLI with:  get Keyspace1.Standard1['jsmith'] ");
	            */
	        } catch (HectorException e) {
	            e.printStackTrace();
	        }
	 }
	
	@Override
	public boolean validateUser(String user, String pass) {
		
		return true;
	}

	@Override
	public String pop3stat(String user, String mailbox) throws MailAppDBException {
		
		 //returns a String for pop3 use like "3 2142" (3 Mails with Size(characters of Message TODO calculate octets))
		return ""+getMessageCount(user, mailbox)+" "+getMailboxSizeCount(user, mailbox);
	}


	@Override
	public void deleteMessages(String user, String mailbox,
			ArrayList<MailAppMessage> deleteList) {

		int deleteCount = deleteList.size();
		int deletedSize = 0;
		for(MailAppMessage msg : deleteList){
			
			try{
			templateMailboxes.deleteColumn(user+keyDelimiter+mailbox, msg.getTimeuuid());
			templateSizetable.deleteColumn(user+keyDelimiter+mailbox, msg.getTimeuuid());
			deletedSize=deletedSize+msg.getSize();
				}catch (HectorException he) {
				
				he.printStackTrace();
			}
			
		}
		try{
			Mutator<String> mutator = HFactory.createMutator(ksp, StringSerializer.get());
			mutator.decrementCounter(user+keyDelimiter+mailbox, counterTableName, counterColumnName, deleteCount);
			mutator.decrementCounter(user+keyDelimiter+mailbox, counterTableName, counterMailboxSizeColumnName, deletedSize);
		}catch (HectorException he) {
			
			he.printStackTrace();
		}
		
		  
		
	}

	@Override
	public int getMessageCount(String user, String mailbox) throws MailAppDBException{
			
		 CounterQuery<String, String> counterQuery = new ThriftCounterColumnQuery<String, String>(
	                ksp, StringSerializer.get(), StringSerializer.get()).
				    setKey(user+keyDelimiter+mailbox).setColumnFamily(counterTableName).setName(counterColumnName);
			
			QueryResult<HCounterColumn<String>> qr = counterQuery.execute();
		HCounterColumn<String> hcc = qr.get();
		if(hcc!=null){
			
		    return Integer.valueOf(hcc.getValue().toString());
		}else return 0;

	}
	
	public long getMailboxSizeCount(String user, String mailbox) {
		
		 CounterQuery<String, String> counterQuery = new ThriftCounterColumnQuery<String, String>(
	                ksp, StringSerializer.get(), StringSerializer.get()).
				    setKey(user+keyDelimiter+mailbox).setColumnFamily(counterTableName).setName(counterMailboxSizeColumnName);
			
			QueryResult<HCounterColumn<String>> qr = counterQuery.execute();
		
			HCounterColumn<String> hcc = qr.get();
			if(hcc!=null){
		    return hcc.getValue();
			}else return 0;

	}
	
	public  MailAppMessage getMessageByID(String user, String mailbox,  String timeUUID) throws MailAppDBException{
		
		//System.out.println("Cass: getting message with id "+timeUUID);
		ColumnQuery<String, String, String> columnQuery = HFactory.createColumnQuery(ksp, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
			.setKey(user+keyDelimiter+mailbox).setColumnFamily(mailboxTableName).setName(timeUUID);
		
		HColumn<String, String> col= columnQuery.execute().get();
		//System.out.println("Cass: retrieved column: "+col.getName()+" value: "+col.getValue());
		MailAppMessage resultMessage = new MailAppMessage(col.getValue(),col.getName());
	
		return resultMessage;
		
		
	}
	public  ArrayList<MailAppMessage> getMessagesUIdList(String user, String mailbox) throws MailAppDBException{
		
		ArrayList<MailAppMessage> resultMessageList = new ArrayList<MailAppMessage>();
		
		SliceQuery<String, String, String> query = HFactory.createSliceQuery(ksp, StringSerializer.get(),
			    StringSerializer.get(), StringSerializer.get()).
			    setKey(user+keyDelimiter+mailbox).setColumnFamily(sizeTableName)
			    .setRange(null, null, false, maxMessagecount);
		QueryResult<ColumnSlice<String, String>> qr = query.execute();
		
		for(HColumn<String, String> col : qr.get().getColumns()){
			long size = col.getValue().getBytes().length;
			String timeID = col.getName();
			MailAppMessage onlyIDandSizeMsg = new MailAppMessage(timeID, size);
			resultMessageList.add(onlyIDandSizeMsg);
		}
		
		return resultMessageList;

	}
	
	private void printDebug(String s){
		if(debug.equalsIgnoreCase("true"))
		 System.out.println("DEBUG CassandraDbConnector: "+s);
	}

}
