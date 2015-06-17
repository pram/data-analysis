package com.naughtyzombie.dataanalysis.csv;

import com.naughtyzombie.dataanalysis.utils.TextArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class CSVReducer extends Reducer<LongWritable, TextArrayWritable, TextArrayWritable, NullWritable> {

    public void reduce(LongWritable key, Iterable<TextArrayWritable> values, Context context)
            throws IOException, InterruptedException {
        for (TextArrayWritable val : values) {
            context.write(val, NullWritable.get());
        }
    }
}