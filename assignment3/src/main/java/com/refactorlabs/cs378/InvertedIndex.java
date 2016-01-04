package com.refactorlabs.cs378;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.LinkedList;
import java.util.Arrays;

/**
 * Author: David Barron (d.barron91@utexas.edu)
 * EID: db25633
 * CSID: davidbar
 * Class: CS 378 Franke
 */
public class InvertedIndex {


	public static class MapClass extends Mapper<Object, Text, Text, Object> {

		// Local variables for email address and email ID
		private Text address = new Text();
		private Text id = new Text();

		//@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String ref = "";  // reference type
			String temp;
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			if(tokenizer.hasMoreTokens() && tokenizer.nextToken().equals("Message-ID:")){
				id.set(tokenizer.nextToken());  //set email ID
			}else{
				id.set("IDnotFound");   // Shouldn't get here, but let's be safe
			}
			
			boolean cont = true;     // Toggle for x-from: 
			
			while (cont && tokenizer.hasMoreTokens()) {
				
				temp = tokenizer.nextToken().toLowerCase(); 
				temp = temp.replaceAll(",","");      // remove any commas 
				
				if (temp.equals("x-from:")) { cont = false; }   // done with email
				else if (temp.equals("to:")) { ref = temp; }
				else if (temp.equals("from:")) { ref = temp; }
				else if (temp.equals("cc:")) { ref = temp; }
				else if (temp.equals("bcc:")) { ref = temp; }
				else if (temp.contains("@")){

					address.set(ref.concat(temp));
					context.write(address, id);					
				}			
			}
		}
	}

	public static class ReduceClass extends Reducer<Text, Object, Text, Object> {

		private Text outValues = new Text();  // for output array string
		
		//@Override
		//@SuppressWarnings("unchecked")
		public void reduce(Text key, Iterable<Object> values, Context context)
				throws IOException, InterruptedException {
	
			LinkedList<Object> list = new LinkedList<Object>();
			
			for (Object value : values) {
				if (!list.contains(value)){      // don't add duplicate ID's
					list.add(new Text(value.toString()));
				}
			}
			
			Object[] array = new Object[list.size()]; 
			array = list.toArray();  // linked lists don't have a sort method
			Arrays.sort(array);      // and am being lazy with Big O.
			outValues.set(Arrays.toString(array));
			
			context.write(key, outValues);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf, "InvertedIndex");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(InvertedIndex.class);

        // Set the outputs for the Map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
		
		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

		// Set the input and output file formats.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Grab the input file and output directory from the command line.
		FileInputFormat.addInputPath(job, new Path(appArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);
	}
}
