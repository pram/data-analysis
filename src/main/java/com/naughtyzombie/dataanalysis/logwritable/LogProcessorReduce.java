package com.naughtyzombie.dataanalysis.logwritable;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class LogProcessorReduce extends
        Reducer<Text, LogWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();
    private MultipleOutputs mos;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        mos = new MultipleOutputs(context);
    }

    public void reduce(Text key, Iterable<LogWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (LogWritable val : values) {
            sum += val.getResponseSize().get();

            mos.write("timestamps", key, val.getTimestamp());
        }
        result.set(sum);
        mos.write("responsesizes", key, result);
    }

    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException {
        mos.close();
    }
}