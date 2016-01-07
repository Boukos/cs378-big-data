package com.refactorlabs.cs378;

//package com.refactorlabs.cs378.assign2;

import com.google.common.collect.Maps;
//import com.refactorlabs.cs378.utils.DoubleArrayWritable;
//import com.refactorlabs.cs378.utils.LongArrayWritable;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * MapReduce program to collect word statistics (per input document).
 *
 * @author David Barron (d.barron91@utexas.edu)
 */
public class WordStatistics extends Configured implements Tool{

	/**
	 * The Map class for word statistics.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the word statistics example.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, AvroValue<WordStatisticsData>> {

		private static final Integer INITIAL_COUNT = 1;

		/**
		 * Local variable "word" will contain a word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as data for each word output.
		 */
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();   // line ot read in
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
				int count = entry.getValue().intValue();   // count of times word appeared in doc
				word.set(entry.getKey());
				
				WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
				
				builder.setDocumentCount(1);  // set one doc read in
				builder.setTotalCount(count);    // set times word encountered 
				builder.setSumOfSquares(count*count);  // set square count val for future Var calculation
				
				// emit for words in one email
				context.write(word, new AvroValue(builder.build()));
				context.getCounter("Mapper Counts", "Output Words").increment(1L);
			}
		}
	}

	/**
	 * The Reduce class for word statistics.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the word statistics example.
	 */
	public static class ReduceClass extends Reducer<Text, AvroValue<WordStatisticsData>, AvroKey<Pair<CharSequence, WordStatisticsData>>, NullWritable> {

		//@Override
		public void reduce(Text key, Iterable<AvroValue<WordStatisticsData>> values, Context context)
				throws IOException, InterruptedException {

			// for writing to AVRO object fields
			long docCount = 0;
			long totalCount = 0;
			long sumOfSquares = 0;
			double mean = 0;
			double variance = 0;

			context.getCounter("Reducer Counts", "Input Words").increment(1L);

			//add up the specified values to calculate Mean and Var
			for (AvroValue<WordStatisticsData> value : values) {
				docCount += value.datum().getDocumentCount();
				totalCount += value.datum().getTotalCount();
				sumOfSquares += value.datum().getSumOfSquares();
			}
			
			//calculate...
			mean = (double)totalCount/docCount;
			variance = (double)sumOfSquares/docCount-mean*mean;
			
			WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
			
			//set fields for new AVRO object
			builder.setDocumentCount(docCount);
			builder.setTotalCount(totalCount);
			builder.setSumOfSquares(sumOfSquares);
			builder.setMean(mean);
			builder.setVariance(variance);
			
			// final emit for word statistics
			context.write(new AvroKey<Pair<CharSequence, WordStatisticsData>>
							(new Pair<CharSequence, WordStatisticsData>(key.toString(), builder.build())),
								NullWritable.get());
		}
	}

	/**
	 * The run() method is called (indirectly) from main(), and contains all the job
	 * setup and configuration.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: WordStatistics <input path> <output path>");
			return -1;
		}

		Configuration conf = getConf();
		Job job = new Job(conf, "WordStatistics");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordStatistics.class);
		// Use this JAR first in the classpath (We also set a bootstrap script in AWS)
		conf.set("mapreduce.user.classpath.first", "true");

		// Specify the Map
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(Text.class);
		AvroJob.setMapOutputValueSchema(job, WordStatisticsData.getClassSchema());

		// Specify the Reduce
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(ReduceClass.class);
		AvroJob.setOutputKeySchema(job,Pair.getPairSchema(Schema.create(Schema.Type.STRING), WordStatisticsData.getClassSchema()));
		job.setOutputValueClass(NullWritable.class);

		// Grab the input file and output directory from the command line.
		String[] inputPaths = appArgs[0].split(",");
		for ( String inputPath : inputPaths ) {
			FileInputFormat.addInputPath(job, new Path(inputPath));
		}
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

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
		int res = ToolRunner.run(new Configuration(), new WordStatistics(), args);
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