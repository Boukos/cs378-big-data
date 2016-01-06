##Assignment 12 - Price Range Analysis

For assignment 12 you'll use several of the patterns/techniques you've coded over the semester to collect some data about users whose activities are recorded in user sessions. In particular, the goal is to collect data on the price ranges of vehicles of interest to users based on which vehicles they clicked on (clicked on a result on an SRP (search results page) to view the VDP (vehicle details page)). This can help answer questions like:

* For users searching for a vehicle around $15K, how broad is the price range they consider?
* For users searching for a vehicle around $45K, how broad is the price range they consider?

In user sessions we have some of the data we need. In particular, we can extract from user sessions the IDs of the vehicles that each user viewed in a VDP. The piece of information we don't have in our user sessions is the price for each of these vehicles. So we'll have to join with a new data source to get this information. The data provided for this assignment is:

* User sessions (in an Avro container file)
* A text file (CSV) that has IDs and the price of the associated vehicle.

Step 1 (filtering and inverted index). To join user VDP views with the price information, we need to identify the common key: ID. The ID/price file is already "keyed" by ID, but user sessions are not, so we'll need to create an inverted index with ID keys, and userIds as the values. User sessions can be read as an AvroKeyInputFormat, with key type AvroKey<Pair<CharSequence, Session>> and value type NullWritable. The Avro schema file (session.avsc) used to create these sessions can be found on Canvas under Files / Assignment 12. This map-reduce job reads user sessions and outputs a text file with:

* key -> ID from the VDP the user viewed.
* value -> the userId.

Step 2 (reduce-side join). Join the file created in Step 1 with the ID/price file. The output of this job will be a text file with:

* key -> userId.
* value -> price of a vehicle viewed on a VDP by the user.

Step 3 (summarization). Aggregate the price data for each user and compute the following statistics for the prices of the vehicles the user viewed on a VDP:

* Price count, min, max, mean, median, standard deviation, skewness, and kurtosis.

Full credit will be given for a correct solution that implements a solution with three separate map-reduce jobs. Extra credit given for a solution that uses job chaining so that all jobs are accomplished with a single step on your AWS cluster.

Recommedations:

I have provided a small version of the user session file and the ID/price file. Get your solution working on this small data before running on the large data set.

Implement you solution as separate jobs first, before trying the job chaining approach.

For the statistics output of Step3, use the utility class we developed early in the semester: DoubleArrayWritable.

To collect the statistics in Step 3, use the Java class: DescriptiveStatistics. It can be found in the Apache Commons Math library, which you can included by adding the following dependency to your pom.xml file:

    <dependency>
        <groupId>org.apache.commons</groupId>
        <artifactId>commons-math3</artifactId>
        <version>3.3</version>
    </dependency>

Data files:

* User sessions: s3n://utcs378/data/dataSet12small/part-r-00000.avro
* ID/price: s3n://utcs378/data/dataSet12small/recordIdPrice.txt
* User sessions: s3n://utcs378/data/dataSet12large/part-r-00000.avro
* ID/price: s3n://utcs378/data/dataSet12large/recordIdPrice.txt

Required elements:

* Price range statistics for each user, taken from the large user session file.

Artifacts to submit:

* Assignment12Build.zip - all files (Java, pom.xml, .avsc) in the directory structure required by maven and buildable with your pom.xml file.
* Assignment12Code.zip - all files (Java, avsc) in a flat directory for easy inspection for grading.
* Assignment12Output.txt - the output file containing the price statistics for each userId.