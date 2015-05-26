package com.naughtyzombie.dataanalysis.logwritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogProcessorMap extends Mapper<Object, LogWritable, Text, LogWritable> {


    public void map(Object key, LogWritable value, Context context)
            throws IOException, InterruptedException {

        context.write(value.getUserIP(), value);
    }

}