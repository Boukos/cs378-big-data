package com.refactorlabs.cs378.utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordStatisticsWritable implements Writable{

	private long docCount;
	private long totalCount;
	private long sumOfSquares;
	private double mean;
	private double variance;
	
	public WordStatisticsWritable(){
		this.docCount  = 0;
		this.totalCount = 0;
		this.sumOfSquares = 0;
		this.mean = 0;
		this.variance = 0;
	}
	
	public WordStatisticsWritable(long doc,long tot,long sum,double mean,double var){
		this.docCount  = doc;
		this.totalCount = tot;
		this.sumOfSquares = sum;
		this.mean = mean;
		this.variance = var;
	}
	
    public void write(DataOutput out) throws IOException {
        out.writeLong(docCount);
        out.writeLong(totalCount);
        out.writeLong(sumOfSquares);
        out.writeDouble(mean);
        out.writeDouble(variance);
    }
	
    public void readFields(DataInput in) throws IOException {
        docCount = in.readLong();
        totalCount = in.readLong();
        sumOfSquares = in.readLong();
        mean = in.readDouble();
        variance = in.readDouble();
    }
    
    public static WordStatisticsWritable read(DataInput in) throws IOException {
    	WordStatisticsWritable w = new WordStatisticsWritable();
        w.readFields(in);
        return w;
    }
    
    @Override
    public String toString(){
    	return docCount+", "+totalCount+", "+sumOfSquares+", "+mean+", "+variance;
    }
    
    public Object[] getValueArray(){
    	Object[] arr = new Object[5];
    	
    	arr[0] = new Long(docCount);
    	arr[1] = new Long(totalCount);
    	arr[2] = new Long(sumOfSquares);
    	arr[3] = new Double(mean);
    	arr[4] = new Double(variance);
    	
    	return arr;
    }
    
    @Override
	public boolean equals(Object o) {
		if (!(o instanceof WordStatisticsWritable)) {
			return false;
		}
		
		Object[] temp = ((WordStatisticsWritable)o).getValueArray();
		
		if(((Long)temp[0]).longValue() != docCount)     { return false;}
		if(((Long)temp[1]).longValue() != totalCount)   { return false;}
		if(((Long)temp[2]).longValue() != sumOfSquares) { return false;}
		if(((Double)temp[3]).longValue() != mean)       { return false;}
		if(((Double)temp[4]).longValue() != variance)   { return false;}
		
		return true;
    }
}
