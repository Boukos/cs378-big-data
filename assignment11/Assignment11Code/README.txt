David Barron
EID: db25633
CSID: davidbar
CS 378 Franke
Assignment 11

The Input for this jar still takes in the class name as the first argument, then the output path as second argument.

	EX: com.refactorlabs.cs378.CustomInput s3://utcs378/db25633/zAsgn11_26

The output value is a custom Writable with the following format:

 	<DocumentCount>,<SumofTotal>,<SumOfSquares>,<Mean>,<Variance>

In the case of the map out value for message statistics, SumofTotal is simply the message length.