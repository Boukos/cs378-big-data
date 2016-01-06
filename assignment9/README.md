##Assignment 9 - Filtering

__Goals:__

For assignment 9, you'll apply some of the filtering techniques we've discussed.

Start with your solution for assignment 8, and remove the logic for replicated join (DistributedCacahe, and mapping ZIP codes to DMA codes - we'll just leave the DMA code field in Impression and Lead as the default value).

Keep the code for MultipleOutputs, identifying session type, and writing the sessions to the output file corresponding to their type (bouncer, browser, searcher, and submitter).

We'll apply filtering techniques in the reducer to limit the number of sessions output. In particular:

* Submitter sessions - output all these sessions
* Searcher sessions - first, select only those sessions that have a Carfax action (VIEWED_CARFAX_REPORT, or VIEWED_CARFAX_REPORT_UNHOSTED). From these, select a random sample at N% (each session has a N% chance of being selected).
* Browser sessions - select a random sample at N%
* Bouncer sessions - select a random sample at M%, where M = N / 10

The sample percentage N should be specified on the command line, and communicated to the reducer(s) via the configuration object. Examples of how to do this can be found in the textbook.

__Required elements:__

* Two mappers, one for impressions, one for leads
* MultipleOutputs, with sessions written to the output determined by the session type
* Filters as described above
* Specify the sample percentage on the command line (use the value 10) and communicate it to the reducer via the configuration object.

__Artifacts to submit:__

* Assignment9Build.zip - all files (Java, avsc, pom.xml) in the directory structure required by maven and buildable with your pom.xml file.
* Assignment9Code.zip - all files (Java, avsc) in a flat directory for easy inspection for grading
* Assignment9Counts.txt - a text file that shows the line counts from the four output files (you can capture the output from "wc -l", or just type the file up by hand.
* Assignment9Bouncer.txt - output file containing the bouncer session output
* Assignment9Browser.txt - output file containing the browser session output
* Assignment9Searcher.txt - output file containing the searcher session output