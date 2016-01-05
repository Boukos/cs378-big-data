package com.refactorlabs.cs378;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.refactorlabs.cs378.CustomInput.MapClass;
import com.refactorlabs.cs378.CustomInput.ReduceClass;
import com.refactorlabs.cs378.WordStatisticsWritable;

public class CustomInput extends Configured implements Tool{
	
	//private final static LongWritable ONE = new LongWritable(1);
	private final static String[] WORDS = {"The", "quick", "brown", "fox", "jumped", "over", "the", "lazy", "dog", "sir"};
	
	public static class MapClass extends Mapper<LongWritable, Text, Text, WordStatisticsWritable> {
		
		private String MSG_STS = "MESSAGE_STATISTICS";
		private Text word = new Text("");
		private WordStatisticsWritable stats = new WordStatisticsWritable(0,0,0,0,0);
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] arr = value.toString().split(" ");
			
			// statistics initialized for message generated
			word.set(MSG_STS);
			stats.set(1,arr.length,arr.length*arr.length,0,0);
			context.write(word, stats);
			
			// statistics initialized for each individual word in message generated
			int[] wordCounts = countArray(arr);
			
			for(int i=0; i < WORDS.length; i++){
				if(wordCounts[i] != 0){
					word.set(WORDS[i]);
					stats.set(1, wordCounts[i], wordCounts[i]*wordCounts[i], 0, 0);
					context.write(word,stats);
				}
			}
		}
		
		// returns array containing count of each string in WORDS present in randomly generated string
		// int at index 0 corresponds to string "The" in WOrds, etc. 
		public static int[] countArray(String[] arr){
			
			int[] out = new int[WORDS.length];
			
			for(int i = 0; i < WORDS.length; i++){
				out[i] = countWords(arr,WORDS[i]);
			}
			
			return out;
		}
		
		// count occurrence of a string in WORDS in the randomly generated array
		public static int countWords(String[] arr, String in){
			int out = 0;
			
			for(int i=0; i<arr.length;i++){
				if(in.equals(arr[i])){
					out++;
				}
			}
			
			return out;
		}
	}
	
	public static class CombineClass extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {
		
		private long count1 = 0;
		private long count2 = 0;
		private long count3 = 0;
		private Object[] arr;
		
		@Override
		public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context)
				throws IOException, InterruptedException {
			
			count1 = 0;
			count2 = 0;
			count3 = 0;
			
			for (WordStatisticsWritable value : values) {
				arr = value.getValueArray();
				count1 += ((Long)arr[0]).longValue();
				count2 += ((Long)arr[1]).longValue();
				count3 += ((Long)arr[2]).longValue();
			}
			context.write(key, new WordStatisticsWritable(count1,count2,count3,0,0));
		}
	}
	
	public static class ReduceClass extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {
		
		private long count = 0;
		private long total = 0;
		private long sumOfSqr = 0;
		private double mean = 0;
		private double variance = 0;
		private Object[] arr;
		private WordStatisticsWritable outVal = new WordStatisticsWritable();
		
		@Override
		public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context)
				throws IOException, InterruptedException {
			
			count = 0;
			total = 0;
			sumOfSqr = 0;
			mean = 0;
			variance = 0;
			
			for (WordStatisticsWritable value : values) {
				
				arr = value.getValueArray();
				count += ((Long)arr[0]).longValue();
				total += ((Long)arr[1]).longValue();
				sumOfSqr += ((Long)arr[2]).longValue();
			}	
			
			mean = (double)total/count;
			variance = (double)sumOfSqr/count-mean*mean;
			
			outVal.set(count, total, sumOfSqr, mean, variance);
			
			context.write(key,outVal);
		}	
	}
	
	// Copied from book
	public static class FakeInputSplit extends InputSplit implements Writable{
		
		public void readFields(DataInput arg0) throws IOException{
			//Does nothing
		}
		
		public void write(DataOutput arg0) throws IOException{
			//Does nothing
		}
		
		public long getLength() throws IOException, InterruptedException{
			return 0;
		}
		
		public String[] getLocations() throws IOException, InterruptedException{
			return new String[0];
		}
	}
	
	public static class RandomTextInputFormat extends InputFormat<LongWritable,Text>{
		
		public List<InputSplit> getSplits(JobContext job) throws IOException{
			
			int numSplits = job.getConfiguration().getInt("numMapTasks", -1);
			ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
			for(int i = 0; i < numSplits; ++i){
				splits.add(new FakeInputSplit());
			}
			return splits;
		}
		
		public RecordReader<LongWritable,Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException{
			
			RandomTextRecordReader rr = new RandomTextRecordReader();
			rr.initialize(split,context);
			return rr;
		}
		
		public static void setNumMapTasks(Job job, int i){
			job.getConfiguration().setInt("numMapTasks", i);
		}
		
		public static void setNumRecordPerTask(Job job, int i){
			job.getConfiguration().setInt("numRecordsPerTask", i);
		}
	}
	
	public static class RandomTextRecordReader extends RecordReader<LongWritable,Text>{
		
		private int numRecordsToCreate = 0;
		private int numCreatedRecords = 0;
		private LongWritable key = new LongWritable(0);
		private Text value = new Text();
		private static Random rand = new Random();
		
		public void initialize(InputSplit split, TaskAttemptContext context) 
				throws IOException, InterruptedException{
			
			this.numRecordsToCreate = context.getConfiguration().getInt("numRecordsPerTask", -1);
		}
		
		public boolean nextKeyValue() throws IOException, InterruptedException{
			
			if(numCreatedRecords < numRecordsToCreate){
				
				numCreatedRecords++;
				key.set(numCreatedRecords);
				
				double r = (rand.nextGaussian()*10)+50;
				int len = (int)r;
				
				if(len < 1){len = 1;}
				if(len > 99){len = 99;}
				
				value.set(getRandString(len));
				
				return true;
			}else{
				return false;
			}
		}
		
		// create String from WORDS values by randomly choosing len number of strings
		public static String getRandString(int len){
			
			String out = "";
			
			int index = rand.nextInt(10);
			
			out = out+WORDS[index];
			
			for(int i = 1; i < len; i++){
				index = rand.nextInt(WORDS.length);
				out = out+" "+WORDS[index];
			}
			
			return out;
		}
		
		public LongWritable getCurrentKey() throws IOException, InterruptedException{
			return key;
		}
		
		public Text getCurrentValue() throws IOException, InterruptedException{
			return value;
		}
		
		public float getProgress() throws IOException, InterruptedException{
			return (float)numCreatedRecords/(float)numRecordsToCreate;
		}
		
		public void close() throws IOException{
			// Does nothing
		}
	}
	
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.err.println("Usage: CustomInput <input path> <output path>");
			System.err.println(Arrays.toString(args));
			return -1;
		}
		
		Configuration conf = getConf();
		Job job = new Job(conf,"CustomInput");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		System.out.println(Arrays.toString(appArgs));
		
		//Identify jar to send to machines
		job.setJarByClass(CustomInput.class);
		
		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(WordStatisticsWritable.class);
		
		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setCombinerClass(CombineClass.class);
		job.setReducerClass(ReduceClass.class);
		
		// Set the input and output file formats.
		job.setInputFormatClass(RandomTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Set some more stuff
		RandomTextInputFormat.setNumMapTasks(job,10);
		RandomTextInputFormat.setNumRecordPerTask(job,100000);
		
		//Set output path
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));
		
		//Wait for completion
		job.waitForCompletion(true);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		printClassPath();
		int res = ToolRunner.run(new Configuration(), new CustomInput(), args);
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
