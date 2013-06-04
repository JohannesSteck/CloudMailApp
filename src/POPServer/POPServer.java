package POPServer;


	import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Properties;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.buffer.SimpleBufferAllocator;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.logging.LogLevel;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.filter.stream.StreamWriteFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

import DBCommunication.DbConnector;
import DBCommunication.DbConnectorFactory;
import DBCommunication.MailAppDBException;

public class POPServer {
		
		private static int PORT;
		private static int timeout;
		public static DbConnectorFactory dbf;
		public static Properties props=new Properties();
		
	    public static void main( String[] args ) throws IOException
	    {
	    	//read properties to determine DB and Pop port
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
			PORT=Integer.valueOf(props.getProperty("popserver.port", "110"));
			timeout=Integer.valueOf(props.getProperty("popserver.connectionTimeoutSec", "10"));
				
	    	//Set up Mina
	        IoBuffer.setUseDirectBuffer(false);
	        IoBuffer.setAllocator(new SimpleBufferAllocator());
	        
	        LoggingFilter log = new LoggingFilter();
	        LogLevel logLvl;
	        if(props.getProperty("popserver.debug", "false").equalsIgnoreCase("true")){
	        logLvl = LogLevel.DEBUG;
	        }else{
	        logLvl = LogLevel.NONE;
	        }
	        log.setMessageReceivedLogLevel(logLvl);
	        log.setMessageSentLogLevel(logLvl);
	        log.setSessionClosedLogLevel(logLvl);
	        log.setSessionCreatedLogLevel(logLvl);
	        log.setSessionIdleLogLevel(logLvl);
	        log.setSessionOpenedLogLevel(logLvl);
	        
	        IoAcceptor acceptor = new NioSocketAcceptor();
	        acceptor.getFilterChain().addLast("logger",log );
	        acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(new TextLineCodecFactory(Charset.forName("US-ASCII"))));
	        acceptor.getFilterChain().addLast("stream", new StreamWriteFilter() );
	        acceptor.setHandler(new POPServerHandler(props));
	        acceptor.getSessionConfig().setIdleTime( IdleStatus.BOTH_IDLE, timeout );
	        acceptor.bind(new InetSocketAddress(PORT));
	        

	    }
	    
	    private static HashMap<IoSession, POP3Session> sessionMap = new HashMap<IoSession, POP3Session>();
	    
	    public static void registerSession(IoSession session, POP3Session popSession){
	    	sessionMap.put(session, popSession);
	    }
	    public static void deleteSession(IoSession session){
	    	sessionMap.remove(session);
	    }
	    public static HashMap<IoSession, POP3Session> getSessionMap(){
	    	return sessionMap;
	    }
	    public static DbConnector getDbConnector() throws MailAppDBException{
	    	return dbf.createDBConnectorInstance();
	    }
	    
	}
