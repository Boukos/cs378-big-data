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
import com.refactorlabs.cs378.DoubleArrayWritable;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.google.common.collect.Maps;
//import com.refactorlabs.cs378.SortedSessions.ImpressionMapper;
//import com.refactorlabs.cs378.SortedSessions.LeadMapper;
//import com.refactorlabs.cs378.SortedSessions.JoinReducer;
//import com.refactorlabs.cs378.WordCount.ReduceClass;








import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
//import org.apache.avro.mapred.AvroMultipleInputs;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner; 









//import java.awt.List;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.List;
import java.net.URL;
import java.net.URLClassLoader;

public class PriceRangeAnalysis extends Configured implements Tool{
	
	public static class AvroMapper 
		extends Mapper<AvroKey<Pair<CharSequence,Session>>, NullWritable, LongWritable, Text> {
		
		private Text userID = new Text();
		private LongWritable vdpID = new LongWritable();
		private Session.Builder inBuild = Session.newBuilder();
		
		public void map(AvroKey<Pair<CharSequence,Session>> key, NullWritable value, Context context) 
				throws IOException, InterruptedException {
			
			inBuild = Session.newBuilder(key.datum().value());    // get Session
			List impressions = inBuild.getImpressions();   // get impressions
			
			userID.set((String)inBuild.getUserId());
			
			while(impressions != null && !impressions.isEmpty()){
				
				Impression tempImp = (Impression)impressions.remove(0);
				
				if(tempImp.getImpressionType() == ImpressionType.VDP){
					long tempVDPID = tempImp.getId().remove(0).longValue(); // get vdp ID
					vdpID.set(tempVDPID);
					context.write(vdpID, userID);
				}
			}
		}
	}
	
	public static class PriceMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		
		private Text price = new Text();
		//private LongWritable price = new LongWritable();
		private LongWritable vdpID = new LongWritable();
		
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
			
			String line = value.toString();
			String[] vals = line.split(",");
			vdpID.set(Long.parseLong(vals[0]));
			price.set(vals[1]);
			context.write(vdpID,price);
		}
	}
	
	public static class JoinReducer extends Reducer<LongWritable,Text,Text,LongWritable>{
		
		private Text userID = new Text();
		private LongWritable price = new LongWritable();
		
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			price.set(0);     // reset the price 
			
			List<String> bag = new ArrayList<String>();  // for userID collection
			
			for(Text value : values){      // first find the price
				String val = value.toString();
				if(val.matches("[0-9]+")){    // found the price
					price.set(Long.parseLong(val));
				}else{
					bag.add(val);      // otherwise it's a userID, add to collection
				}
			}

			Object[] arr = bag.toArray();
			if(!(price.get() == 0)){
				for(Object str : arr){     // now write out key and value
				
					userID.set((String)str);
					context.write(userID, price);
				}
			}
		}
	}
	
	public static class RangeMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
		
		private LongWritable price = new LongWritable();
		private Text userID = new Text();
		
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
			
			String tempStr = value.toString();
			
			String[] in = tempStr.split("\t");  // split userID and price
			
			if(in.length == 2){  // let's be safe before setting and writing
				userID.set(in[0]);
				price.set(Long.parseLong(in[1]));
				context.write(userID, price);
			}
		}	
	}
	
	public static class RangeReducer extends Reducer<Text,LongWritable,Text,DoubleArrayWritable>{
		
		private DoubleArrayWritable builder = new DoubleArrayWritable();
		private DescriptiveStatistics stats = new DescriptiveStatistics();
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context )
			throws IOException, InterruptedException {
			
			System.out.println("IN RANGEREDUCER");
			System.out.println("key "+key.toString());
			
			double count = 0;   // count of prices viewed
			double[] info = new double[8];  // temp array to hold stats info
			stats.clear();      // clear the Statistics object
			
			System.out.println("info defined");
			
			for(LongWritable value : values){
				
				count++;
				stats.addValue((double)value.get());  // add price
			}
			
			info[0] = count;
			info[1] = stats.getMin();
			info[2] = stats.getMax();
			info[3] = stats.getMean();
			info[4] = getMedian(stats.getSortedValues());
			info[5] = stats.getStandardDeviation();
			info[6] = stats.getSkewness();
			info[7] = stats.getKurtosis();
			
			DoubleArrayWritable outVal = builder.make(info);
			context.write(key, outVal);
		}
		
		public static double getMedian(double[] arr){
			
			double out = 0;
			
			if(arr.length%2 == 1){
				out = arr[arr.length/2];
			}else{
				out = (arr[arr.length/2] + arr[(arr.length/2)-1])/2;
			}
			
			return out;
		}
	}
	
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: PriceRangeAnalysis <input path> <output path>");
			return -1;
		}
		
		Configuration conf = getConf();
		
		Job job = new Job(conf, "PriceRangeAnalysis");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		job.setJarByClass(PriceRangeAnalysis.class);
		conf.set("mapreduce.user.classpath.first", "true");
		
		MultipleInputs.addInputPath(job,new Path(appArgs[0]), AvroKeyInputFormat.class, AvroMapper.class);
		AvroJob.setInputKeySchema(job,Pair.getPairSchema(Schema.create(Schema.Type.STRING), Session.getClassSchema()));
		MultipleInputs.addInputPath(job,new Path(appArgs[1]), TextInputFormat.class, PriceMapper.class);
		
		//job.setOutputKeyClass(LongWritable.class);
		//job.setOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(JoinReducer.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(appArgs[2]));
		
		boolean goodToGo = job.waitForCompletion(true);
		
		if(!goodToGo){
			System.err.println("ERROR: Initial job failed to complete succesfully.");
			return -1;
		}
		
		Job analysisJob = submitAnalysisJob(conf, new Path(appArgs[2].concat("/part-r-00000")),
				new Path(appArgs[2].concat("_range_analysis")));
		
		while(!analysisJob.isComplete()){
			Thread.sleep(5000);
		}
		
		if(analysisJob.isSuccessful()){
			System.out.println("Analysis job completed succesfully.");
		}else{
			System.out.println("Analysis job failed!");
		}
		
		return 0;
	}
	
	private static Job submitAnalysisJob(Configuration conf, Path inputDir, Path outputDir)throws Exception{
		
		Job job = new Job(conf,"priceRangeJob");
		job.setJarByClass(PriceRangeAnalysis.class);
		
		job.setMapperClass(RangeMapper.class);
		job.setReducerClass(RangeReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleArrayWritable.class);
		
		//job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job,inputDir);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.submit();
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		printClassPath();
		int res = ToolRunner.run(new Configuration(), new PriceRangeAnalysis(), args);
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
