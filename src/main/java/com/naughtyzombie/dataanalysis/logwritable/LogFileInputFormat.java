package com.naughtyzombie.dataanalysis.logwritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class LogFileInputFormat extends FileInputFormat<LongWritable, LogWritable>{

	@Override
	public RecordReader<LongWritable, LogWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		return new LogFileRecordReader();
	}

}