package com.naughtyzombie.dataanalysis.csv;

import com.naughtyzombie.dataanalysis.utils.TextArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public final class CSVMapReduce {

    public static void main(String... args) throws Exception {
        runJob(args[0], args[1]);
    }

    public static void runJob(String input, String output) throws Exception {
        Configuration conf = new Configuration();
        conf.set(CSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",");
        conf.set(CSVOutputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ":");

        Job job = Job.getInstance(conf,"CSV MapReduce");
        job.setJarByClass(CSVMapReduce.class);
        job.setMapperClass(CSVMapper.class);
        job.setReducerClass(CSVReducer.class);
        job.setInputFormatClass(CSVInputFormat.class);
        job.setOutputFormatClass(CSVOutputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(TextArrayWritable.class);

        job.setOutputKeyClass(TextArrayWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        Path outPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outPath);

        outPath.getFileSystem(conf).delete(outPath, true);

        job.waitForCompletion(true);
    }
}
