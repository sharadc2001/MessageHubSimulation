package com.ibm.injest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
//import java.nio.charset.Charset;
//import java.nio.file.Files;
//import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Level;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messagehub.samples.MessageHubJavaSample;
import com.messagehub.samples.env.MessageHubCredentials;
import com.messagehub.samples.env.MessageHubEnvironment;
import com.messagehub.samples.env.MessageList;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

@ApplicationPath("rest")
@Path("IngestService")
public class IngestService  extends Application{
	  private String topic="SampleTopic";
	  private String filePath="";
	  private String line="";
	  private  ExecutorService executor = Executors.newFixedThreadPool(5);

	  @GET
	  @Path("SimulateIngestion")
	  @Produces("application/json")
	  public void SimulateIngestion(@QueryParam("load")@DefaultValue("10")String load,
			                     @QueryParam("batchsize")@DefaultValue("1")String batchsize) {
		     System.out.println("fetch data called");
		     String val=load.trim();
             System.out.println("batchsize:: " +batchsize);
             int batch=Integer.parseInt(batchsize);

			 System.out.println("load:: " +load + "load.equalIgnoreCase("+load+"):: " +load.equalsIgnoreCase(val));

			 if(load.equalsIgnoreCase("10")){
				 filePath="data/Sample_10.csv";
	       	  }else if(load.equalsIgnoreCase("100")){
	       		filePath="data/Sample_100.csv";
	       	  }else if(load.equalsIgnoreCase("1000")){
	       		filePath="data/Sample_1000.csv";
	       	  }else if(load.equalsIgnoreCase("10000")){
	       		filePath="data/Sample_10000.csv";
	       	  }else if(load.equalsIgnoreCase("100000")){
	       		filePath="data/Sample_100000.csv";
	       	  }else if(load.equalsIgnoreCase("5000")){
	       		filePath="data/Sample_5000.csv";  
	       	  }else if(load.equalsIgnoreCase("50000")){
	       		filePath="data/Sample_50000.csv";
	       	  }else if(load.equalsIgnoreCase("500000")){
	       		filePath="data/Sample_500000.csv";  
	       	  }
	       	  
			  if(batch==1)pushData(filePath);
			  else pushBatchData(filePath,batch);

	  } 
	  
	  public void pushData(String filePath){
		   try{
			    System.out.println("push data line by line");
			    MessageHubJavaSample proxy=new MessageHubJavaSample(topic);
                int linecount=getLineCount(filePath);
	        	ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	        	InputStream fileStream = classLoader.getResourceAsStream(filePath);
	        	InputStreamReader r = new InputStreamReader(fileStream);
	        	BufferedReader br = new BufferedReader(r);
	        	System.out.println("Inside Push Data:: " +fileStream);
                int total=linecount;
                int count=0;
                int i=0;
	        	while((line=br.readLine())!=null){
	        		 count++;
	                 if(i<=total){
	        		    System.out.println("Injesting Data:: " +line);
	        		    proxy.InjestData(line);
	        		    i++;
	                 }
	        	}	
                --count;
                System.out.println("Total Lines Ingested:: " +count);
		   }catch(Exception t){t.printStackTrace();}
		    finally{System.out.println("Ingested all the data*****************");}
	  }

	  public void pushBatchData(String datapath,int batch){
		  try{
			    System.out.println("push data in batches");
			    MessageHubJavaSample proxy=new MessageHubJavaSample(topic);
			    int linecount=getLineCount(datapath);
	        	ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	        	InputStream fileStream = classLoader.getResourceAsStream(datapath);
	        	InputStreamReader reader = new InputStreamReader(fileStream);
	        	BufferedReader br = new BufferedReader(reader);
	        	System.out.println("Inside Push Data:: " +fileStream);
	        	
			    
	        	MessageList list = new MessageList();
            	//Insert content in array
            	//i variable will run till total data count
            	//count variable will keep track of batch size and after every 
            	//batch size completion it will reset back to 0. 
     			int total=linecount;
				int batchsize=batch;
	            int count=0;
	            int diff=0;
	            int i=0;
	            System.out.println("Total Batch Size:: " +batchsize);
	            while((line=br.readLine())!=null){
	            	list.push(line);
	            	count++;
                if(i<total){
	            	if(count==batchsize){
	            		diff=total-i-1;
	            		System.out.println("Ingested "+count+ " lines");
	            		System.out.println("Total left:: " +diff);
	            		proxy.InjestData(list);//Ingest batch of data
	            		list=new MessageList();
	            		if(diff<batchsize){
	            			batchsize=diff;
	            		}
	            		count=0;
	            	}
	            	i++;
	              }
	            }	 
                --i;
                System.out.println("Total Lines Ingested:: " +i);
		  }catch(Exception t){
			  t.printStackTrace();
		  }
	  }
	  
	  private int getLineCount(String filePath){
		  int count=0;
		  try{
	        	ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	        	InputStream fileStream = classLoader.getResourceAsStream(filePath);
	        	InputStreamReader r = new InputStreamReader(fileStream);
	        	BufferedReader br = new BufferedReader(r);
	        	System.out.println("Inside Push Data:: " +fileStream);
	        	while((line=br.readLine())!=null){
                       ++count;
	        	}				  
		  }catch(Exception t){t.printStackTrace();}
		  return count;
	  }


	  
/*	  public void pushDataThreaded(String topic,String datapath){
		   try{
	            Runnable worker = new WorkerThread(topic,datapath);
	            executor.execute(worker);
		   }catch(Exception t){t.printStackTrace();}
		    finally{executor.shutdown();while (!executor.isTerminated()){}}
	  }
*/	  
}
