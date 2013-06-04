package MailAppUtils;


import java.util.Map;
import java.util.Scanner;
import java.util.UUID;

public class MailAppMessage {
	
	private Map<String, String> sqsMessageAttributes;
	private String from=null;
	private String to=null;
	//private String data="test data mail \r\n zeile 2 \r\nzeile3\r\nzeile 4";
	private long size=0;
	private String time=null;
	
	private String wholeMessage=null;
	
	private String timeuuid=null;

	public MailAppMessage(String message, Map<String, String> sqsMessageAttributes){
		//build message when storing in mailbox
		//System.out.println("building new Message------------------");
		//System.out.println("------------whole Message-----------");
		//System.out.println(message);
		//System.out.println("------------*************-----------");
		setSqsMessageAttributes(sqsMessageAttributes);
		
		//System.out.println("Message attributes: ");
		//System.out.println(sqsMessageAttributes);
		
		setWholeMessage(message);
		
		//first line is sender		
		Scanner scanner = new Scanner(message);
		String line = scanner.nextLine();
		
		setFrom(line);
		//System.out.println("setting sender: "+line);
		//second is receiver
		line = scanner.nextLine();
		setTo(line);
		//System.out.println("setting receiver: "+line);
		
		//set Time
		//TODO process Attribute collection with more Attributes than just timestamp
		String sentTimestamp = sqsMessageAttributes.get(sqsMessageAttributes.keySet().iterator().next());
		//System.out.println("message timestamp: "+sentTimestamp);
		setTime(sentTimestamp);			
		setTimeuuid();
		
		this.size=wholeMessage.length();
		//System.out.println("message timeuuid: "+getTimeuuid());
		//System.out.println("--------------------DONE building new Message------------------");
	}
	
	public MailAppMessage(String message, String timeID){
		//build message when retrieving from mailbox (for POP3)
		//System.out.println("building new Message------------------");
		//System.out.println("------------whole Message-----------");
		//System.out.println(message);
		//System.out.println("------------*************-----------");
		
		setWholeMessage(message);
		
		//first line is sender		
		Scanner scanner = new Scanner(message);
		String line = scanner.nextLine();
		
		setFrom(line);
		//System.out.println("setting sender: "+line);
		//second is receiver
		line = scanner.nextLine();
		setTo(line);
		//System.out.println("setting receiver: "+line);

		//TODO make more generic
		String[] timeIdArray = timeID.split("_");
		setTime(timeIdArray[0]);			
		setTimeuuid(timeID);
		this.size=wholeMessage.length();
		//System.out.println("message timeuuid: "+getTimeuuid());
		//System.out.println("--------------------DONE building new Message------------------");
	}

	//construct Message with just timeuuid and size, other fields are null
	public MailAppMessage(String timeID, long size) {
		this.timeuuid=timeID;
		this.size=size;
		
	}
	//construct Message with just timeuuid, other fields are null
	public MailAppMessage(String timeID) {
		this.timeuuid=timeID;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	public String getFrom() {
		return from;
	}

	public void setTo(String to) {
		this.to = to;
	}

	public String getTo() {
		return to;
	}
/*
	public void setData(String data) {
		this.data = data;
	}

	public String getData() {
		return data;
	}
*/
	public void setTime(String time) {
		this.time = time;
		
	}

	public String getTime() {
		return time;
	}

	public void setTimeuuid() {
		
		this.timeuuid = time+"_"+UUID.randomUUID();
	}
	public void setTimeuuid(String timeuuid) {
		
		this.timeuuid = timeuuid;
	}

	public String getTimeuuid() {
		
		return timeuuid;
	}

	public void setWholeMessage(String wholeMessage) {
		this.wholeMessage = wholeMessage;
	}

	public String getWholeMessage() {
		return wholeMessage;
	}
	//TODO remove sender and receiver from first 2 lines in message
	public String getWholeMessageWITHOUT2LINES() {
		return wholeMessage;
	}

	public void setSqsMessageAttributes(Map<String, String> sqsMessageAttributes) {
		this.sqsMessageAttributes = sqsMessageAttributes;
		
	}

	public Map<String, String> getSqsMessageAttributes() {
		return sqsMessageAttributes;
	}

	public int getSize() {
		//return Size characters
		return (int)size;
	}

	
}
