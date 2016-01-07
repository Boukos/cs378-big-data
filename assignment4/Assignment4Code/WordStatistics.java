package com.refactorlabs.cs378.assign2;

import com.google.common.collect.Maps;
import com.refactorlabs.cs378.utils.DoubleArrayWritable;
import com.refactorlabs.cs378.utils.LongArrayWritable;
import com.refactorlabs.cs378.utils.WordStatisticsWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.nio.*;
//import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * MapReduce program to collect word statistics (per input document).
 *
 * @author David Barron (d.barron91@utexas.edu)
 */
public class WordStatistics {

	/**
	 * The Map class for word statistics.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the word statistics example.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, WordStatisticsWritable> {

		private static final Integer INITIAL_COUNT = 1;

		/**
		 * Local variable "word" will contain a word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as data for each word output.
		 */
		private Text word = new Text();
		//private WordStatisticsWritable stats = new WordStatisticsWritable();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			context.getCounter("Mapper Counts", "Input Documents").increment(1L);

			Map<String, Integer> wordCountMap = Maps.newHashMap();
			// For each word in the input document, determine the number of times the
			// word occurs.  Keep the current counts in a hash map.
			while (tokenizer.hasMoreTokens()) {
				String nextWord = tokenizer.nextToken();
				Integer count = wordCountMap.get(nextWord);

				if (count == null) {
					wordCountMap.put(nextWord, INITIAL_COUNT);
				} else {
					wordCountMap.put(nextWord, count.intValue() + 1);
				}
			}
			
			// Create the output value for each word, and output the key/value pair.
			for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
				int count = entry.getValue().intValue();
				word.set(entry.getKey());
				
				context.write(word, new WordStatisticsWritable(1,count,count*count,0,0));
				context.getCounter("Mapper Counts", "Output Words").increment(1L);
			}
		}
	}

	/**
	 * The Reduce class for word statistics.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the word statistics example.
	 */
	public static class ReduceClass extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {

		private WordStatisticsWritable stats = new WordStatisticsWritable();
		
		@Override
		public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context)
				throws IOException, InterruptedException {
	
			Object[] contents;   //get values from custom class
			
			long docCount = 0;
			long totalCount = 0;
			long sumOfSquares = 0;
			double mean = 0;
			double variance = 0;
			
			context.getCounter("Reducer Counts", "Input Words").increment(1L);
			
			for (WordStatisticsWritable value : values) {    // reduce values in custom writable

				contents = value.getValueArray();
				docCount += ((Long)contents[0]).longValue();
				totalCount += ((Long)contents[1]).longValue();
				sumOfSquares += ((Long)contents[2]).longValue();
				
			}
			
			// calculate mean and variance
			mean = (double)totalCount/docCount;
			variance = (double)sumOfSquares/docCount-mean*mean;
	
			context.write(key, new WordStatisticsWritable(docCount,totalCount,sumOfSquares,mean,variance));
		}
	}

	/**
	 * The main method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf, "WordStatistics");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordStatistics.class);

		// Set the output key and value types (for map and reduce).
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WordStatisticsWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(WordStatisticsWritable.class);

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		//job.setCombinerClass(CombinerClass.class);
		job.setReducerClass(ReduceClass.class);

		// Set the input and output file formats.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Grab the input file and output directory from the command line.
		String[] inputPaths = appArgs[0].split(",");
		for ( String inputPath : inputPaths ) {
			FileInputFormat.addInputPath(job, new Path(inputPath));
		}
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);
	}

}
