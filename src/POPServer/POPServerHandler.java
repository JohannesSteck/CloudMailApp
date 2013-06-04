package POPServer;
import java.util.Properties;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;

import DBCommunication.DbConnector;
import DBCommunication.DynamoDB.DynamoDBConnector;




public class POPServerHandler extends IoHandlerAdapter{
	
	
	private Properties props;
	private String debug;
	
	public POPServerHandler(Properties p) {
		super();
		props=p;
		debug=props.getProperty("popserver.debug","false");
	}
	@Override
    public void exceptionCaught( IoSession session, Throwable cause ) throws Exception
    {
        cause.printStackTrace();
    }
    @Override
    public void messageReceived( IoSession session, Object message ) throws Exception
    {
    	
    	POP3Session p3s;
    	if(   			(p3s = POPServer.getSessionMap()			.get(session)) 	!= null  			){
    		p3s.processMessage(message);
    	}else{
    		//Session abgelaufen/existiert nicht?
    	}
       
        
    }
    @Override
    public void sessionIdle( IoSession session, IdleStatus status ) throws Exception
    {
        System.out.println( "IDLE POP Session ID: "+ session.getId()+ " write: "+session.getWrittenMessages()+ " read: "+session.getReadMessages());
        session.write("-ERR Session Timeout");
        session.close(true);
        
    }
    
    @Override
    public void sessionCreated(IoSession session) throws Exception {
    	//create new Session
    	DbConnector db = POPServer.getDbConnector();
    	new POP3Session(session, db, debug);
    	session.write( "+OK POP3 server ready");
    }

}
