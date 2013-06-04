package POPServer;
import java.util.ArrayList;
import java.util.UUID;

import org.apache.mina.core.session.IoSession;

import DBCommunication.DbConnector;
import DBCommunication.MailAppDBException;
import MailAppUtils.MailAppMessage;



public class POP3Session {
	public enum popCommands {USER, PASS, STAT, LIST, RETR, DELE, NOOP, RSET, QUIT/*, APOP, TOP	*/,  UIDL , CAPA};
	public enum popStates {AUTHORIZATION, TRANSACTION, UPDATE};
	
	private popStates currentState;
	UUID id;
	public Object lastClientMessage;
	private IoSession session;
	private String user=null;
	private String pw=null;

	private DbConnector db;
	private ArrayList<MailAppMessage> deleteList = new ArrayList<MailAppMessage>();
	private ArrayList<MailAppMessage> messages;
	private int messagesSize=-1;
	private String targetMailbox = "inbox";
	
	private boolean debug=false;
	
	public POP3Session(IoSession session, DbConnector db, String debug){
		super();
		this.debug=debug.equalsIgnoreCase("true");
		this.db = db;
		id = UUID.randomUUID();
		setPOPState(popStates.AUTHORIZATION);
		this.session = session;
		POPServer.registerSession(session, this);
		//TODO remove
				
	}
	public void processMessage(Object message) throws Exception{
		
		String msg = message.toString();
		//System.out.println("------------incomming message session: "+session.getId()+"------------");
		printDebug("C: "+msg);
		//System.out.println("-----------------------------------------------------------------------");
		
		
		
		switch (currentState) {
		case AUTHORIZATION:
			
			processAuthorizationStateMessage(msg);
			
			break;
		case TRANSACTION:
			
			processTransactionStateMessage(msg);
			
			break;
		case UPDATE:
			
			writeToSession("-ERR in Update State");
			
			break;

		default:
			writeToSession("-ERR No State");
			break;
		}
		
	}


public static boolean validatePOPCommand(String command){
		popCommands[] cmds = popCommands.values();
		for(int i = 0; i< cmds.length;i++){
			//System.out.println(cmds[i]+" =? "+command);
			if(command.toLowerCase().startsWith(cmds[i].toString().toLowerCase())){
				//System.out.println("match!");
				return true;
			}
		
		}
		return false;
	}
	public void setPOPState(popStates ps){
		currentState = ps;
	}
	
	private void processAuthorizationStateMessage(String msg) throws Exception{
		
		String[] msgArray;
		msgArray = msg.split(" ");
		String cmd = msgArray[0];
		if(validatePOPCommand(cmd)){
			if (msgArray.length>1){
				
				if(cmd.toLowerCase().startsWith("user")){
					user = msgArray[1];
					//validate User(Mailbox)
					writeToSession("+OK waiting for password");
				}else if(cmd.toLowerCase().startsWith("pass")&&user!=null){
					//check password
					
					if(db.validateUser(user, pw)){
						setPOPState(popStates.TRANSACTION);
					writeToSession("+OK authentification successful ");
						
					}else{
						writeToSession("-ERR Authentifocation failed");
					}
					
				}else{
					//command not valid in this context
					writeToSession("-ERR error in auth state");
				}
			}else{
				//no argument
				if(cmd.toLowerCase().startsWith("quit")){
					//quit -> dont enter update State!
					//writeToSession("+OK quit atuhentification Bye");
					printDebug("closing Session with id "+session.getId());
					POPServer.deleteSession(session);
					session.close();
				}else if(cmd.toLowerCase().startsWith("capa")){
					writeToSession("+OK List of capabilities follows");
					writeToSession("PLAIN");
					writeToSession(".");
				}else{
					writeToSession("-ERR no argument!");
				}
			}
		}else{
			writeToSession("-ERR unknown command!");
		}
	}
	private void processTransactionStateMessage(String msg) throws MailAppDBException {
		String[] msgArray;
		msgArray = msg.split(" ");
		String cmd = msgArray[0];
		
		if(validatePOPCommand(cmd)){
			if (msgArray.length>1){
				
				if(cmd.toLowerCase().startsWith("list")){
					//list + arg
					getMailsList();

					try{
					MailAppMessage m = messages.get(Integer.valueOf(msgArray[1])-1);
					writeToSession("+OK "+msgArray[1]+" "+m.getSize());
					
					}catch(Exception e){
						writeToSession("-ERR Message with index "+msgArray[1]+" not found");
					}

					
				}
				/*
				 * else if(cmd.toLowerCase().startsWith("uidl")){
				 
					//uidl without arg
					writeToSession("+OK "+msgArray[0]+" "+msgArray[0]+"uidl");
				}
			*/
				else if(cmd.toLowerCase().startsWith("retr")){
					//retr
					getMailsList();
					try{
						MailAppMessage m = getFullMessage(Integer.valueOf(msgArray[1])-1);						
						writeToSession("+OK message "+msgArray[1]+" follows");
						writeToSession(m.getWholeMessageWITHOUT2LINES());
						writeToSession(".");
						}catch(Exception e){
							writeToSession("-ERR Message with index "+msgArray[1]+" not found");
						}
					
					
					
					
				}else if(cmd.toLowerCase().startsWith("uidl")){
						//retr
						getMailsList();
						try{
							MailAppMessage m = messages.get(Integer.valueOf(msgArray[1])-1);
							writeToSession("+OK "+msgArray[1]+" "+m.getTimeuuid());
							}catch(Exception e){
								writeToSession("-ERR");
							}
						
						
						
						
					}else if(cmd.toLowerCase().startsWith("dele")){
					//dele
					getMailsList();
					int msgDeleteIndex = Integer.valueOf(msgArray[1])-1;
					if(messages.size()>0){
						if(!(messages.size()-1<msgDeleteIndex)){
							deleteList.add(	messages.get(msgDeleteIndex));
						}else{
							//for benchmarking stability reasons
							deleteList.add(	messages.get(messages.size()-1));
						}
						writeToSession("+OK message "+msgArray[1]+" marked for deletion");
					}else{
						writeToSession("-ERR message "+msgArray[1]+" not marked for deletion");
					}
											
											
					
				}
			}else{
				//no argument
				if(cmd.toLowerCase().startsWith("stat")){
					//stat					
					String statMsg = db.pop3stat(user, targetMailbox);
					if(statMsg.startsWith("-")){
						statMsg="0 0";
					}
					writeToSession("+OK "
							+statMsg	);
						//	+"\r\n");

				}else if(cmd.toLowerCase().startsWith("list")){
					//list without arg
					getMailsList();
					writeToSession("+OK "+messages.size()+" messages ("+getMessagesSize()+" Bytes)");
					
					for(int i = 0; i< messages.size();i++){
						writeToSession(""+(i+1)+" "+messages.get(i).getSize());
					}
					writeToSession(".");
					
				}
				
				else if(cmd.toLowerCase().startsWith("uidl")){
					getMailsList();
					//uidl without arg
					writeToSession("+OK uidl list follows");
					for(int i = 0; i< messages.size();i++){
						writeToSession(""+(i+1)+" "+messages.get(i).getTimeuuid());
					}
					writeToSession(".");
				}else if(cmd.equalsIgnoreCase("NOOP")){
					//noop
					session.write("+OK");
				}else if(cmd.toLowerCase().startsWith("rset")){
					//rset
					deleteList.clear();
					writeToSession("+OK RSET");
				}else if(cmd.toLowerCase().startsWith("quit")){
					//quit -> enter update State
					processUpdateState();/*
					session.write("+OK quit transaction ...Bye");
					setPOPState(popStates.UPDATE);*/
					
				}else{
					
				session.write("-ERR in transaction state");
				}
			}
		}else{
			session.write("-ERR unknown command");
		}
		
	}
	private void processUpdateState() {
		
		//delete marked Messages
		try {
			if(!deleteList.isEmpty()){
				db.deleteMessages(user, targetMailbox, deleteList);
			}
		} catch (MailAppDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//session.write("+OK Update State...Bye");
		printDebug("deleting marked Messages ("+ deleteList.size() +")...");
		POPServer.deleteSession(session);
		session.close();

	}
	public void writeToSession(String msg){
		
		printDebug("S: "+msg);
		//session.write(msg+"\r\n");
		session.write(msg+(char)13);

		
//reply(this.session, msg);
		
	}
/*	public synchronized void writeToSessionML(String msg){
		
		//System.out.println("S: "+msg);
		//session.write(msg);
		
replyMultiline(this.session, msg);
		
	}
	*/
	
    void reply(IoSession session, String msg) {
    	System.out.println("S: "+msg);
//        session.write(msg + (char)13 + (char)10);
                session.write(msg + (char)13);
//        session.write(msg + "\n");
    }

    void replyMultiline(IoSession session, String content) {
        // todo: handle special case of single line consisting of a .
        session.write(content + "\n");
    }
	
	public String getExampleMIMEMessage(String number){
		//just for testing
	String mimeMsg; 
			mimeMsg="From: John Doe <example@example.com>\n"+
		"MIME-Version: 1.0\n"+
		"Content-Type: multipart/mixed;\n"+
		"       boundary=\"XXXXboundary text\"\n"+
		"\n"+
		"This is a multipart message in MIME format.\n\n"+
		
		"--XXXXboundary text \n"+
		"Content-Type: text/plain\n\n"+		
		"this is the body text\n\n"+
		"from message: "+number+"\n"+
		"--XXXXboundary text \n"+
		"Content-Type: text/plain;\n"+
		"Content-Disposition: attachment;\n"+
		" filename=\"test.txt\"\n\n"+
		
		"this is the attachment text\n"+
		
		"--XXXXboundary text--";
			
		
			
	return mimeMsg;
	}
	private int getMessagesSize(){
		if(messagesSize<=-1){
			if(messages==null){
				return 0;
			}else {
				int size=0;
				for(int i=0;i<messages.size();i++){
					size = size + messages.get(i).getSize();
				}
				messagesSize = size;
				
			}
		}
	return messagesSize;
		

		
	}
	private void getMailsList(){
		if(messages==null){
			//messages = db.getMailbox(user, targetMailbox);
			//retrieve UIDL List first.
			try {
				messages = db.getMessagesUIdList(user, targetMailbox);
			} catch (MailAppDBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	private synchronized MailAppMessage getFullMessage(int index) throws MailAppDBException{
		
		if(messages==null){
			getMailsList();
			}
			//fetch whole message if only ID is loaded up to now
		if(messages.get(index).getWholeMessage()==null){
			 printDebug("message with list index "+index+1+" is null -> loading whole message: "+messages.get(index).getTimeuuid());
			messages.set(index,db.getMessageByID(user, targetMailbox, messages.get(index).getTimeuuid()));
		}
		return messages.get(index);
	}
	
	private void printDebug(String s){
		if(debug)
		System.out.println("POP3 Session id: "+session.getId()+" :: "+s);
		
		
	}
}
