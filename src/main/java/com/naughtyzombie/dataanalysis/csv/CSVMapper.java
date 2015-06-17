package com.naughtyzombie.dataanalysis.csv;

import com.naughtyzombie.dataanalysis.utils.TextArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CSVMapper extends Mapper<LongWritable, TextArrayWritable, LongWritable, TextArrayWritable> {

    @Override
    protected void map(LongWritable key, TextArrayWritable value,Context context)
            throws IOException, InterruptedException {
        context.write(key, value);
    }
}