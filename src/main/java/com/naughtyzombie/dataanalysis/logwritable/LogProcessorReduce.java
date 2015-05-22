package com.naughtyzombie.dataanalysis.logwritable;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogProcessorReduce extends Reducer<Text, LogWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<LogWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (LogWritable val : values) {
            sum += val.getResponseSize().get();
        }
        result.set(sum);
        context.write(key, result);
    }
}