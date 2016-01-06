Assignment 7, User Session join (impressions and leads)
Due Oct 28, 2014 by 9:30am  Points 25  Submitting a file upload
For Assignment 7, you'll implement a join of impression logs and lead logs into a user session. Your mapReduce job will read in individual impression log entries and individual lead log entries, create an Impression object that represents each impression log entry and a Lead object that represents each lead log entry, and assemble these into user sessions.

A user session should be created for each unique userId/apikey combination, as the userId identifies activities by the same user, and an apikey is unique to the site with which the user is interacting.

In order to process different types of input (impression logs and lead logs), use the MultipleInputs class to associate a mapper to each type of input. You'll need to define a mapper that takes impression log entries and outputs a Session containing that Impression. You'll also need to define a mapper that takes lead log entries and outputs a Session containing that Lead.

In addition to the fields from the lead log entry, I've added one field to the Avro schema for a Lead: "vdp_index". This field should contain the index (in the impression array) of the VDP impression with the same id as the id field of the lead. (Note: in a lead log entry, the id field has the name "recordid".) For each lead, examine the array/list of impressions for a VDP impression that has the same id. If you find such an impression, set the vdp_index field of the lead to the index of that impression. If multiple such impressions exist, set the index to the last such impression in the array/list.

The command line you create for your mapReduce app on AWS will now have four arguments on the command line:

your class name
s3n://utcs378/data/dataSet7Impressions.txt
s3n://utcs378/data/dataSet7Leads.txt
your output directory
Required elements:

Make your mapReduce key the userId + ":" + apikey
Define a mapper for impression log entries, and a separate mapper for lead log entries
Use the MultipleInputs class to associate these mappers with the corresponding input file
Map output (for both mappers) should be: Text, AvroValue<Session>
Reduce output should be: AvroKey<Pair<CharSequence, Session>>, NullWritable
Impressions should be ordered by timestamp
The "vdp_index" field in a Lead should have the index of the corresponding VDP impression, if that impression exists. Otherwise, this field should be -1.
I have provided the Avro schema (session.avsc) I want you to use for this assignment. This file can be found on Canvas: Files / Assignment 7.

Also provided (use these if you want):

log entry parser (in the LogUtils class)
Handler interface for Session and Impression (discussed in class)
These file can be found on Canvas: Files / Assignment 7.

dataSet7Impressions.txt and dataSet7Leads.txt are available on Canvas (and have been uploaded to S3). I recommend you download at least dataSet7Leads.txt to familiarize yourself with what is represented in a lead log entry. The fields you need to process are defined in the Avro schema I've provided. Just like for impression log entries, the format of a lead log is:

The letter "L" followed by a tab
parameter/value pairs, where each pair is separated from other pairs by the string "|\t|", and the parameter name and parameter value are separated by ":".
As we did for impressions logs, I recommend that you run your WordCount app against dataSet7Leads.txt, as it will show you all the unique parameter:value pairs and the number of occurrences for each unique parameter:value pair. You only need concern yourself with the lead fields defined in the Avro schema record type "Lead".

Inputs: dataSet7Impressions.txt and dataSet7Leads.txt

Output: Avro text representation of your user session instances

Artifacts to submit:

Assignment7Build.zip - all files (Java, avsc, pom.xml) in the directory structure required by maven and buildable with your pom.xml file.
Assignment7Code.zip - all files (Java, avsc) in a flat directory for easy inspection for grading
Assignment7Output.txt - output file containing the text representation of your Avro user session objects