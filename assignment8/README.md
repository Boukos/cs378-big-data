##Assignment 8 - Replicated Join and Multiple Outputs

__Goals:__

For this assignment you'll be adding two capabilities we've discussed previously to your session generator app:

* Replicated join: copying small files to all the machines where your tasks (map and reduce) will be running so that each task can load these files and "join" them with the large data set.
* Multiple outputs: from a reducer, send the output to different files based on some characteristic of the data. We saw this done in the "binning" pattern, where mappers write their output to different files based on some aspect of the data. You'll do this from the reducer.

First, the Avro schema for a user session (provided for this assignment) has been updated as follows:

* dma field (text) added to impression
* customer_zip and customer_dma fields added to lead
* vehicle_zip and vehicle_dma fields added to lead

For impressions, you should populate the zip field in the Avro schema when you see the "zip" field or when you see the "listingzip" field in an impression log entry. For leads, you should populate the customer_zip field in the Avro schema when you see the "customer_zip" field in the lead log entry, and populate the vehicle_zip field in the Avro schema when you see the "zip" field in the lead log entry.

__Replicated Join:__ For replicated join, a file that associates a zip with a dma code (three digits) is provided. You should use the Hadoop class DistributedCache to get this file copied to all machines where your map and reduce tasks are running. In the mapper class for impressions and the mapper class for leads, you'll read this file in the setup() method and create a data structure that allows you to look up a dma code given a zip code. As you are processing impressions or leads, if the log entry has a zip code defined, then set the corresponding dma field in the Avro object.

The format of the zip/dma file looks like this:

78701,635

78759,635

67890,456

...

Steps in using DistributedCache (example code in the lecture notes):

* In your run() method:
 * Get the name of the zipToDma mapping file from the command line
 * Tell Hadoop about this file using the addCacheFile() method of DistributedCache. Note that you need to make the call to addCacheFile() after you have created the Configuration object, but before you create the Job object.
* In the setup() method of your mappers (impression and lead):
 * Get the array of filenames (Path[]) using the getLocalCacheFiles() method of DistributedCache
 * Read these files into memory (we only have one such file) to create your data structure that maps zip codes to dma codes.
 

__Multiple Outputs:__ For multiple outputs, you'll examine each user session and classify it into one of these categories: bouncer, browser, searcher, submitter. Given a category, you'll write the user session out to a file determined by the category. To do this, you'll use the Hadoop class MultipleOutputs. (Note: The corresponding class for Avro, namely AvroMultipleOutputs, has the limitation that we cannot use TextOutputFormat. We can only output Avro objects into Avro container files, which are binary files and unreadable.)

*Categorizing sessions:* You should determine the user session category as follows:

	1. If the session has a lead, the category is "submitter"
	2. If the session does not have a lead, and only one impression, the category is "bouncer"
	3. If not "submimtter" or "bouncer", then the session category is "browser" if it has only SRP impressions, otherwise the category is "searcher".

Steps in using MultipleOutputs (example code in the lecture notes, which have been updated):

* In your run() method:
 * Tell Hadoop you'll be using multiple output files by calling the addNamedOutput() method of MultipleOutputs.
 * Also tell Hadoop to enable counters for multiple outputs
* In the setup() method of the reduce class, create an instance of MultipleOutputs
* In the cleanup() method of the reduce class, call the close() method of the MultipleOutputs instance
* In the reduce() method, call the write() method on the MultipleOutputs instance.

Since you are using MultipleOutputs instead of AvroMultipleOuptuts (in order to get readable text output), you will need to change the declaration of your reduce class to specify the following input key/value and output key/value types:

\<Text, AvroValue\<Session>, Text, Text>

In particular, note that you'll be writing the output key (userId:apikey) as a Text object, and you'll be writing out the value (Session) as a Text object. To convert your Session to a Text object, do this:

new Text(session.toString())

For inputs (impression and lead logs), use the data sets from assignment7. I will add new data sets for assignemnt 8, but you can start with the assignment 7 data. There is also a new file that specifies the zip code to dma code mapping: s3n://utcs378/data/dataSet8Zip2Dma.txt

The command line you create for your mapReduce app on AWS will now have five arguments on the command line:

* your class name
* s3n://utcs378/data/dataSet7Impressions.txt
* s3n://utcs378/data/dataSet7Leads.txt
* your output directory
* s3n://utcs378/data/dataSet8Zip2Dma.txt

__Required elements:__

* Make your mapReduce key the userId + ":" + apikey
* Define a mapper for impression log entries, and a separate mapper for lead log entries
* Use the MultipleInputs class to associate these mappers with the corresponding input file
* Map output (for both mappers) should be: Text, AvroValue<Session>
* Reduce output should be: Text, Text
* Use DistributedCache class to send the zip/dma map file to all mappers
* Reducer should write output to a file determined by the categorization of the session
* Impressions should be ordered by timestamp
* The "vdp_index" field in a Lead should have the index of the corresponding VDP impression, if that impression exists. Otherwise, this field should be -1.

I have provided the Avro schema (session.avsc) I want you to use for this assignment. This file can be found on Canvas: Files / Assignment 8.

Output files on AWS will look like this:

* bouncer-r-00000
* browser-r-00000
* searcher-r-00000
* submitter-r-00000

__Artifacts to submit:__

* Assignment8Build.zip - all files (Java, avsc, pom.xml) in the directory structure required by maven and buildable with your pom.xml file.
* Assignment8Code.zip - all files (Java, avsc) in a flat directory for easy inspection for grading
* Assignment8Output.txt - output file containing the text representation of your Avro user session objects