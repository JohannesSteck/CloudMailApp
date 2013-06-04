package DBCommunication;

import java.util.Properties;

import DBCommunication.Cassandra.CassandraDbConnector;
import DBCommunication.DynamoDB.DynamoDBConnector;

public class DbConnectorFactory {

	public Properties props;
	private String useDB;
	
	public DbConnectorFactory(Properties p){
		this.props = p;
		useDB = p.getProperty("useDB");
	}
	
	public DbConnector createDBConnectorInstance() throws MailAppDBException{
		
		String errorMsg="ERROR: ";
		
		DbConnector db = null;

		if(useDB.equalsIgnoreCase("dynamodb")){
		db = new DynamoDBConnector(props);
		db.init();
		}else if(useDB.equalsIgnoreCase("cassandra")){
		db = new CassandraDbConnector(props);
		db.init();
		}else{
			
			if(useDB!=null || useDB.length()>0){
				errorMsg=errorMsg+"DB not recognized: "+useDB;
			}else if(useDB.length()==0){
				errorMsg=errorMsg+"Parameter useDB empty!";
			}else{
				errorMsg=errorMsg+"Error while reading Parameter useDB.";
			}
			
		}

		if(db!=null){
		return db;
		}else{
			System.out.println(errorMsg);
			throw new MailAppDBException();
		}
	}
}
