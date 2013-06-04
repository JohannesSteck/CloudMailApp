package MailAppUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import DBCommunication.MailStoringWorkerStarter;
import POPServer.POPServer;
import SMTPServer.BasicSMTPServer;

public class StartSingleNodeMailApp {

	public static Properties props= new Properties();
	
	public static CapacityLogger mailboxtable_caplogger;
    public static CapacityLogger messagetable_caplogger;
    public static CapacityLogger sizetable_caplogger;
	
	public static void main(String[] args) {
		//Starts the Service with all needed components

		//read properties to determine DB and Pop port
    	String propertiesPath;
    	System.out.println("args length: "+args.length);
    	if(args.length==0){
    		//default properties file    		
    		propertiesPath="MailApp.properties";
    		System.out.println("no Arguments. Default properties file: "+propertiesPath);
    	}else{
    		//only one argument supported (properties file)
	    	propertiesPath=args[0];
    	}		
			
			try{
				System.out.println("loading Properties... file exists: "+new File(propertiesPath).canRead());
				FileInputStream fis = new FileInputStream(propertiesPath);
				
				props.load(fis);
				System.out.println("done.");
			}
			catch (IOException e){
				e.printStackTrace();
				System.exit(0);
			}
		
			//start Pop Server
			try {
			new POPServer();
			
				POPServer.main(args);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//start SMTP Server
			new BasicSMTPServer();
			
			BasicSMTPServer.main(args);
			
			if(!Boolean.parseBoolean(props.getProperty("smtpserver.directdbconnection", "true"))){			
				//start Queue Workers
				System.out.println("Using indirect (SQS-based) Communication from SMTP to DB");
				new MailStoringWorkerStarter();
				MailStoringWorkerStarter.main(args);
			}else{
				System.out.println("Using direct Communication from SMTP to DB");
			}
			
			boolean logCapacity = Boolean.parseBoolean(props.getProperty("dbproperties.dynamodb.logcapacity","false"));
	    	
	    	if(logCapacity){
	    		int logCapacityInterval=Integer.valueOf(props.getProperty("dbproperties.dynamodb.logcapacityInterval","60"));
	    		sizetable_caplogger=new CapacityLogger("sizetable", logCapacityInterval);
	    		mailboxtable_caplogger=new CapacityLogger("countertable", logCapacityInterval);
	    		messagetable_caplogger=new CapacityLogger("messagetable", logCapacityInterval);
	    		sizetable_caplogger.start();
	    		mailboxtable_caplogger.start();
	    		messagetable_caplogger.start();
	    	}
			
	}

}
