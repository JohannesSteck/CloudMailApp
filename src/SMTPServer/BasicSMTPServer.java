package SMTPServer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.subethamail.smtp.server.SMTPServer;



public class BasicSMTPServer {
	private static int PORT;
	private static int timeout;
	private static Properties props= new Properties();

        public static void main(String[] args) {
        	//read properties to determine SMTP port
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
				PORT=Integer.valueOf(props.getProperty("smtpserver.port","25"));
				timeout=Integer.valueOf(props.getProperty("smtpserver.connectionTimeoutSec","10"));
        	
                MyMessageHandlerFactory myFactory = new MyMessageHandlerFactory(props) ;
                SMTPServer smtpServer = new SMTPServer(myFactory);
                smtpServer.setPort(PORT);
                smtpServer.setConnectionTimeout(timeout*1000);
                smtpServer.start();
                
        }
}	