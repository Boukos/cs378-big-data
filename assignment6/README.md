##Assignment 6 - User Sessions

__Goals:__

For Assignment 6, you'll define an Avro object that represents a user session. Your mapReduce job will read in individual log entries, create an Impression object that represents each log entry, and assemble these into user sessions. A user session should be created for each unique userId/apikey combination, as the userId identifies activities by the same user, and an apikey is unique to the site with which the user is interacting.

You should download dataSet6.txt from S3 examine the content to familiarize yourself with what is represented in a log entry. The format of a log entry is:

* The letter "I" followed by a tab
* Parameter/value pairs, where each pair is separated from other pairs by the string "|\t|", and the parameter name and parameter value are separated by ":".

As was discussed in class, determine which fields should be associated with the session in total, and which fields should be associated with an individual impression. The fields you should consider and include in your Avro object are:

* __type__ (enum is already created for you in the provided .avsc file). Values you find for the type parameter should be represented as an enum values as follows:
 * action -> enum value ACTION
 * Email, Phone, email, landing, phone -> enum value VDP
 * no value provided -> enum value SRP
* __action__ (enum is already created for you in the provided .avsc file). Values you find for the action parameter should be represented as an enum values as follows:
 * click -> enum value CLICK
 * no value specified -> enum value PAGE_VIEW
* __action_name__ - create an enum for this field
* __id__ - an array of zero or more longs
* __timestamp__
* __ab__ - store the associated string
* __vertical__ - create an enum for this field
* __start_index__
* __total__
* __domain__
* __lat__
* __lon__
* __address__
* __city__
* __zip__
* __state__
* __phone_type__ - create an enum for this field
* __uid__
* __apikey__
* __uagent__
* __res__
* __activex__

Your mapReduce job should group impressions together by uid (user ID) and apikey, and organize them in an array ordered by timestamp and add them to the session object.

The easiest way to understand all the values that might occur in a field is to run your WordCount app against dataSet6.txt, as it will show you all the unique parameter:value pairs and the number of occurrences for each unique parameter:value pair.

Input: dataSet6.txt

Output: Avro text representation of your user session instances

__Required elements:__

* Avro defined object representing a user session

__Artifacts to submit:__

* Assignment6Build.zip - all files (Java, avsc, pom.xml) in the directory structure required by maven and buildable with your pom.xml file.
* Assignment6Code.zip - all files (Java, avsc) in a flat directory for easy inspection for grading
* Assignment6Output.txt - output file containing the text representation of your Avro user session objects