package MailAppUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.Date;

public class CapacityLogger extends Thread{

	double writeCap=0;
	double readCap=0;
	double writeCapRounded=0;
	double readCapRounded=0;
	String table;
	int interval;
	String filename;
	DecimalFormat df = new DecimalFormat("0.00");
	
	public CapacityLogger(String tablename, int averageIntervalSeconds) {
		this.interval=averageIntervalSeconds;
		this.table=tablename;
		filename = "capacity_avg_"+interval+"sec_"+tablename+"_"+System.currentTimeMillis()+".txt";
		try {
		    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)));
		    out.println("start time: "+new Date());
		    out.println("interval(sec); "+interval);
		    out.println("averageWriteCapPerSec\t;averageWriteCapRoundedPerSec\t;averageReadCapPerSec\t;averageReadCapRoundedPerSec");
		    out.close();
		} catch (IOException e) {
		    //oh noes!
		}
	}
	public void logReadCapacity(double cap){
		this.readCap=readCap+cap;
		this.readCapRounded=readCapRounded+Math.ceil(cap);
	}
	public void logWriteCapacity(double cap){
		this.writeCap=writeCap+cap;
		this.writeCapRounded=writeCapRounded+Math.ceil(cap);
	}
	
	public void run(){
		while(true){
			try {
				sleep(interval*1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			double writeCapSnap = writeCap;
			double readCapSnap = readCap;
			double writeCapSnapRounded = writeCapRounded;
			double readCapSnapRounded = readCapRounded;
			writeCap=0;
			readCap=0;
			writeCapRounded=0;
			readCapRounded=0;
			double averageWriteCapPerSec = writeCapSnap/interval;
			double averageReadCapPerSec = readCapSnap/interval;
			double averageWriteCapRoundedPerSec = writeCapSnapRounded/interval;
			double averageReadCapRoundedPerSec = readCapSnapRounded/interval;
			
					
			try {
			    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(new File(filename), true)));
			    out.println(""+new Date()+"\t;"+df.format(averageWriteCapPerSec)+"\t;"+df.format(averageWriteCapRoundedPerSec)+"\t;"+df.format(averageReadCapPerSec)+"\t;"+df.format(averageReadCapRoundedPerSec));
			    out.close();
			} catch (IOException e) {
			    e.printStackTrace();
			}
		}
		
	}

	
}
