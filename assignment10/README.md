##Assignment 10 - Job Chaining

__Goal:__

For assignment 10, you'll apply the techniques we've discussed for job chaining. You'll also use Avro container files (binary files).

Start with your solution for assignment 9, and remove the filtering and sampling logic. Keep the code for MultipleOutputs, identifying session type, and writing the sessions to the output corresponding to their type (bouncer, browser, searcher, and submitter).

So to start, we have a map-reduce job that has two mappers, one that reads impression log entries and one that reads lead log entries, creates user sessions and writes those session to four different output files, depending on the session type. Note: I've uploaded a latest version of session.avsc to Canvas ( Files / Solutions / Assignment 9) if you would like to use it.

__Using Avro Container (Binary) File:__

First, change the output format for sessions from TextFileOutputFormat to AvroKeyOutputFormat. You'll need to use the AvroMultipleOutputs class in place of the MultipleOutputs class.

Your reduce class declaration should look like this:

`public class ReduceClass
    extends Reducer<Text, AvroValue<Session>,
        AvroKey<Pair<CharSequence, Session>>, NullWritable>`

__Job Chaining:__

After sessions have been created and output to the four files (depending on the session type), create and launch two additional jobs that run in parallel and do the following:

* Read the file of bouncer sessions and count the impression types. Each line of the output will be an impression type (string) and a count.
* Read the file of searcher sessions and compute the VDP click thru rate. This will be the number of VDP impressions divided by the total number of IDs found on SRP impressions. The one line of output should be: total number of VDP impressions, total number of IDs on SRP impressions, and the click thru rate.

__Some hints on what you'll need to do:__

* Define two additional jobs (in the run() method) that will perform the above processing on sessions
* Call AvroJob.setInputKeySchema() to define the input schema these jobs will be reading
* Use job.submit() to launch these two jobs, then monitor their progress.

__Required elements:__

* Two mappers, one for impressions, one for leads
* AvroMultipleOutputs, with sessions written to the output file determined by the session type
* Run in parallel:
 * Job that counts the impression types in bouncer sessions
 * Job that counts the VDP click thru rate on searcher sessions

__Artifacts to submit:__

* Assignment10Build.zip - all files (Java, avsc, pom.xml) in the directory structure required by maven and buildable with your pom.xml file.
* Assignment10Code.zip - all files (Java, avsc) in a flat directory for easy inspection for grading
* Assignment10BouncerCounts.txt - a text file that shows the impression typrs and counts in bouncer sessions
* Assignment10SearcherCount.txt - a text file with the counts described above