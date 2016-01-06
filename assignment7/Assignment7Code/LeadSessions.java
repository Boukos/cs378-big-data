package com.refactorlabs.cs378;

/*
 * David Barron(db25633)
 * CSID: davidbar
 * CS 378 Franke
 */

import com.refactorlabs.cs378.sessions.Session;
import com.refactorlabs.cs378.sessions.Impression;
import com.refactorlabs.cs378.sessions.ImpressionType;
import com.refactorlabs.cs378.sessions.ActiveX;
import com.refactorlabs.cs378.sessions.Action;
import com.refactorlabs.cs378.sessions.ActionName;
import com.refactorlabs.cs378.sessions.PhoneType;
import com.refactorlabs.cs378.sessions.Vertical;
import com.refactorlabs.cs378.sessions.BidType;
import com.refactorlabs.cs378.sessions.Lead;
import com.refactorlabs.cs378.sessions.LeadType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import com.google.common.collect.Maps;
import com.refactorlabs.cs378.LeadSessions.ImpressionMapper;
import com.refactorlabs.cs378.LeadSessions.LeadMapper;
import com.refactorlabs.cs378.LeadSessions.JoinReducer;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner; 

import java.awt.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.util.StringTokenizer;
import java.net.URL;
import java.net.URLClassLoader;

public class LeadSessions extends Configured implements Tool{

	// Class to map out Session objects with an Impression set
	public static class ImpressionMapper extends Mapper<LongWritable, Text, Text, AvroValue<Session>> {
		
		private Text logID = new Text();  //map out key
		private Session.Builder builder = Session.newBuilder();   // map out session
		private Impression.Builder imp = Impression.newBuilder();  // goes inside session
		private String apikey = "";    // temporary strings to construct logID
		private String uid = "";
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String temp = value.toString().replace("|","");   // workaround, split("|\\t|") tokenizing string to chars		
			String[] line = temp.split("\\t");  //split log by |TAB| format
			
			// set Key for Mapper output
			for( int i = 1; i < line.length; i++ ){    // find user id and apikey
				if(line[i].startsWith("apikey")) {apikey = line[i];}
				if(line[i].startsWith("uid")) {uid = line[i];}
			}
			
			uid = uid.substring(3);     // remove leading chars before ':'
			apikey = apikey.substring(7);
					
			logID.set(uid.concat(":").concat(apikey));    // end set key
			
			if(line[0].equals("I")){
				for( int i = 1; i < line.length; i++ ){
					String[] splitEntry = line[i].split(":",2);  //get parameter and value
					
					// populate appropriate Session fields
					if (splitEntry.length == 2){
						if(splitEntry[0].equals("apikey")){
							builder.setApiKey(splitEntry[1]);
						}else if(splitEntry[0].equals("uid")){
							builder.setUserId(splitEntry[1]);
						}else if(splitEntry[0].equals("res")){
							builder.setResolution(splitEntry[1]);
						}else if(splitEntry[0].equals("uagent")){
							builder.setUserAgent(splitEntry[1]);
						}else if(splitEntry[0].equals("activex")){
							if(splitEntry[1].equalsIgnoreCase("enabled")){
								builder.setActivex(ActiveX.ENABLED);
							}    //populate appropriate Impression fields
						}else if(splitEntry[0].equals("type")){
							if(splitEntry[1].equalsIgnoreCase("action")){
								imp.setImpressionType(ImpressionType.ACTION);
							}else if(splitEntry[1].equalsIgnoreCase("email") || splitEntry[1].equalsIgnoreCase("phone")
									|| splitEntry[1].equalsIgnoreCase("landing")){
								imp.setImpressionType(ImpressionType.VDP);
							}else if(splitEntry[1].equalsIgnoreCase("thankyou")){
								imp.setImpressionType(ImpressionType.THANK_YOU);
							}
						}else if(splitEntry[0].equals("action")){
							if(splitEntry[1].equalsIgnoreCase("click")){
								imp.setAction(Action.CLICK);
							}
						}else if(splitEntry[0].equals("action_name")){
							if(splitEntry[1].equalsIgnoreCase("dealer_page_viewed")){
								imp.setActionName(ActionName.DEALER_PAGE_VIEWED);
							}else if(splitEntry[1].equalsIgnoreCase("dealer_website_viewed")){
								imp.setActionName(ActionName.DEALER_WEBSITE_VIEWED);
							}else if(splitEntry[1].equalsIgnoreCase("more_photos_viewed")){
								imp.setActionName(ActionName.MORE_PHOTOS_VIEWED);
							}else if(splitEntry[1].equalsIgnoreCase("viewed_carfax_report")){
								imp.setActionName(ActionName.VIEWED_CARFAX_REPORT);
							}else if(splitEntry[1].equalsIgnoreCase("viewed_carfax_report_unhosted")){
								imp.setActionName(ActionName.VIEWED_CARFAX_REPORT_UNHOSTED);
							}else {
								imp.setActionName(ActionName.UNKNOWN);
							}
						}else if(splitEntry[0].equals("timestamp")){
							imp.setTimestamp(Long.parseLong(splitEntry[1]));
						}else if(splitEntry[0].equals("ab")){
							imp.setAb(splitEntry[1]);
						}else if(splitEntry[0].equals("vertical")){
							if(splitEntry[1].equalsIgnoreCase("cars")){
								imp.setVertical(Vertical.CARS);
							}
						}else if(splitEntry[0].equals("start_index")){
							imp.setStartIndex(Integer.parseInt(splitEntry[1]));
						}else if(splitEntry[0].equals("total")){
							imp.setTotal(Integer.parseInt(splitEntry[1]));
						}else if(splitEntry[0].equals("domain")){
							imp.setDomain(splitEntry[1]);
						}else if(splitEntry[0].equals("lat")){
							imp.setLat(Double.parseDouble(splitEntry[1]));
						}else if(splitEntry[0].equals("lon")){
							imp.setLon(Double.parseDouble(splitEntry[1]));
						}else if(splitEntry[0].equals("address")){
							imp.setAddress(splitEntry[1]);
						}else if(splitEntry[0].equals("city")){
							imp.setCity(splitEntry[1]);
						}else if(splitEntry[0].equals("zip")){
							imp.setZip(splitEntry[1]);
						}else if(splitEntry[0].equals("state")){
							imp.setState(splitEntry[1]);
						}else if(splitEntry[0].equals("phone_type")){
							if(splitEntry[1].equalsIgnoreCase("tracked")){
								imp.setPhoneType(PhoneType.TRACKED);
							}
						}else if(splitEntry[0].equals("id")){
							//@SuppressWarnings("rawtypes")
							LinkedList list = toLinkedList(splitEntry[1]);
							imp.setId(list);
						}
					}
				}
			}
			
			LinkedList myList = new LinkedList();  // wrapper list to set Impression
			
			myList.add(imp.build());  
			
			builder.setImpressions(myList);    // set Impression
			
			context.write(logID, new AvroValue(builder.build()));   //final session write out
		}
		
		// input string formated as string longs separated by commas
		//@SuppressWarnings({ "rawtypes", "unchecked" })
		public LinkedList toLinkedList(String str){
			
			String[] arr = str.split(",");
			LinkedList out = new LinkedList();
			
			for(int i = 0; i < arr.length; i++){
				out.add(Long.parseLong(arr[i],10));
			}
			return out;
		}
	}
	
	// Class to map out Session objects with a Lead set
	public static class LeadMapper extends Mapper<LongWritable, Text, Text, AvroValue<Session>> {
		
		private Text logID = new Text();  //map out key
		private Session.Builder builder = Session.newBuilder();   // map out session
		private Lead.Builder lead = Lead.newBuilder();  // goes inside session
		private String apikey = "";    // temporary strings to construct logID
		private String userid = "";
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String temp = value.toString().replace("|","");   // workaround, split("|\\t|") tokenizing string to chars		
			String[] line = temp.split("\\t");  //split log by |TAB| format
			
			// set Key for Mapper output
			for( int i = 1; i < line.length; i++ ){    // find user id and apikey
				if(line[i].startsWith("apikey")) {apikey = line[i];}
				if(line[i].startsWith("userid")) {userid = line[i];}
			}
			
			userid = userid.substring(7);     // remove leading chars before ':'
			apikey = apikey.substring(7);
					
			logID.set(userid.concat(":").concat(apikey));    // end set key
			
			if(line[0].equals("L")){
				for( int i = 1; i < line.length; i++ ){
					String[] splitEntry = line[i].split(":",2);  //get parameter and value
					
					// populate appropriate Session fields
					if (splitEntry.length == 2){
						if(splitEntry[0].equals("apikey")){
							builder.setApiKey(splitEntry[1]);
						}else if(splitEntry[0].equals("userid")){
							builder.setUserId(splitEntry[1]);
						}else if(splitEntry[0].equals("res")){
							builder.setResolution(splitEntry[1]);
						}else if(splitEntry[0].equals("uagent")){
							builder.setUserAgent(splitEntry[1]);
						}else if(splitEntry[0].equals("activex")){
							if(splitEntry[1].equalsIgnoreCase("enabled")){
								builder.setActivex(ActiveX.ENABLED);
							}   // populate appropiate Lead fields
						}else if(splitEntry[0].equals("lead_id")){
							lead.setLeadId(Long.parseLong(splitEntry[1]));
						}else if(splitEntry[0].equals("type")){
							if(splitEntry[1].equals("good")){
								lead.setType(LeadType.GOOD);
							}else if(splitEntry[1].equals("duplicate")){
								lead.setType(LeadType.DUPLICATE);
							}else if(splitEntry[1].equals("range")){
								lead.setType(LeadType.RANGE);
							}else if(splitEntry[1].equals("error")){
								lead.setType(LeadType.ERROR);
							}
						}else if(splitEntry[0].equals("bid_type")){
							if(splitEntry[1].equals("sale")){
								lead.setBidType(BidType.SALE);
							}else if(splitEntry[1].equals("other")){
								lead.setBidType(BidType.OTHER);
							}
						}else if(splitEntry[0].equals("advertiser")){
							lead.setAdvertiser(splitEntry[1]);
						}else if(splitEntry[0].equals("campaign_id")){
							lead.setCampaignId(splitEntry[1]);
						}else if(splitEntry[0].equals("ab")){
							lead.setAb(splitEntry[1]);
						}else if(splitEntry[0].equals("recordid")){   // I think this is the correct ID we need to compare...
							lead.setId(Long.parseLong(splitEntry[1]));
						}else if(splitEntry[0].equals("amount")){
							lead.setAmount(Float.parseFloat(splitEntry[1]));
						}else if(splitEntry[0].equals("revenue")){
							lead.setRevenue(Float.parseFloat(splitEntry[1]));
						}else if(splitEntry[0].equals("test")){
							if(splitEntry[1].equals("true")){
								lead.setTest(true);
							}
						}
					}
				}
			}
			
			LinkedList myList = new LinkedList();  // wrapper list to set Impression
			
			myList.add(lead.build());  
			
			builder.setLeads(myList);    // set Lead
			
			context.write(logID, new AvroValue(builder.build()));   //final session write out
		}
	}
	
	// Class to join Impressions and Leads from same Session
	public static class JoinReducer extends Reducer<Text, AvroValue<Session>, AvroKey<Pair<CharSequence, Session>>, NullWritable> {
		
		private Session.Builder finalBuild = Session.newBuilder();

		@Override
		public void reduce(Text key, Iterable<AvroValue<Session>> values, Context context)
				throws IOException, InterruptedException {
			
			Session.Builder outBuild;
			Impression temp; 
			Lead temp2;
			
			LinkedList impList = new LinkedList();  // used to set session Impressions
			LinkedList leadList = new LinkedList(); // used to set session Leads
			
			for (AvroValue<Session> value : values) {
				
				outBuild = Session.newBuilder(value.datum());   //copy Session
				
				if(outBuild.getImpressions() != null){
					finalBuild = Session.newBuilder(value.datum());
					temp = Impression.newBuilder(outBuild.getImpressions().get(0)).build();  //retrieve Impression
					impList.add(temp);   // add Impression
				}else if(outBuild.getLeads() != null){
					temp2 = Lead.newBuilder(outBuild.getLeads().get(0)).build();  //retrieve Lead
					leadList.add(temp2); // add Lead
				}
			}
			
			if(impList.size() == 0 ){
				for (AvroValue<Session> value : values) {
					finalBuild = Session.newBuilder(value.datum());
					break;
				}
			}
			
			impList = sortImpressionsList(impList);  // sort Impressions by timestamp
			leadList = setLeadIndex(impList.toArray(),leadList.toArray());  // set Lead indexes
			
			finalBuild.setImpressions(impList);   // set impressions
			finalBuild.setLeads(leadList);        // set leads
			
			context.write(new AvroKey<Pair<CharSequence, Session>>
			(new Pair<CharSequence, Session>(key.toString(), finalBuild.build())),
				NullWritable.get());
			
		}
		
		// sorts Linkedlist containing Impressions based on timestamp
		public LinkedList sortImpressionsList(LinkedList in){
			int index = 1;
					
			LinkedList out = new LinkedList();
					
			long a;  // longs used to compare timestamps
			long b;
					
			Object temp;
					
			Object[] arr = in.toArray(); // convert to array for easier sort
					
			while(index < arr.length){
				a = (long)((Impression)arr[index-1]).getTimestamp();
				b = (long)((Impression)arr[index]).getTimestamp();
						
				if( a-b > 0){
					temp = arr[index-1];
					arr[index-1] = arr[index];
					arr[index] = temp;
					index = 1;
				}else{
					index++;
				}
			}
					
			for(int i = 0; i<arr.length;i++){  // array elems back to LinkedList
				out.add(arr[i]);
			}
					
			return out;
		}
		
		// goes through each lead and searches the id arrays in each impression to determine if index must be set
		public LinkedList setLeadIndex(Object[] impressions, Object[] leads){
			
			LinkedList out = new LinkedList();   // for return
			long leadID;     // temp long for index search
			Long[] impressionIDs;   // temp array of Impression ids
			boolean prim;           // toggle to assure only one vdp_index set occurs
			
			for(int i = 0; i < leads.length; i++){
				leadID = (long)((Lead)leads[i]).getId();
				prim = true;
				for(int j = 0; j < impressions.length; j++){
					impressionIDs = (Long[])((Impression)impressions[j]).getId().toArray(new Long[((Impression)impressions[j]).getId().size()]);
					if(prim && containsLong(leadID,impressionIDs)){
						((Lead)leads[i]).setVdpIndex(j);
						prim = false;
					}
				}
			}
			
			for(int i = 0; i<leads.length;i++){  // array elems back to LinkedList
				out.add(leads[i]);
			}
			return out;
		}
		
		// searches an array of Longs to determine if in long is in array
		public boolean containsLong(long in, Long[] arr){
			
			for(int i = 0; i < arr.length; i++){
				if(in == arr[i].longValue()){
					return true;
				}
			}	
			return false;
		}
	}
	
	
	/**
	 * The run() method is called (indirectly) from main(), and contains all the job
	 * setup and configuration.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: LeadSessions <input path> <output path>");
			return -1;
		}

		Configuration conf = getConf();
		Job job = new Job(conf, "LeadSessions");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(LeadSessions.class);
		// Use this JAR first in the classpath (We also set a bootstrap script in AWS)
		conf.set("mapreduce.user.classpath.first", "true");

		MultipleInputs.addInputPath(job,new Path(appArgs[0]), TextInputFormat.class,ImpressionMapper.class);
		MultipleInputs.addInputPath(job,new Path(appArgs[1]), TextInputFormat.class,LeadMapper.class);
		
		// Specify the Map
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(Text.class);
		AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

		// Specify the Reduce
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(JoinReducer.class);
		AvroJob.setOutputKeySchema(job,Pair.getPairSchema(Schema.create(Schema.Type.STRING), Session.getClassSchema()));
		job.setOutputValueClass(NullWritable.class);

		// Grab the input file and output directory from the command line.
		//String[] inputPaths = appArgs[0].split(",");
		//for ( String inputPath : inputPaths ) {
		//	FileInputFormat.addInputPath(job, new Path(inputPath));
		//}
		FileOutputFormat.setOutputPath(job, new Path(appArgs[2]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);

		return 0;
	}

	/**
	 * The main method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */
	public static void main(String[] args) throws Exception {
		printClassPath();
		int res = ToolRunner.run(new Configuration(), new LeadSessions(), args);
		System.exit(res);
	}
	
	public static void printClassPath() {
		ClassLoader cl = ClassLoader.getSystemClassLoader();
		URL[] urls = ((URLClassLoader) cl).getURLs();
		System.out.println("classpath BEGIN");
		for (URL url : urls) {
			System.out.println(url.getFile());
		}
		System.out.println("classpath END");
	}
}
