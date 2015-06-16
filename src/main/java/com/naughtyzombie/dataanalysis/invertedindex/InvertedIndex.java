package com.naughtyzombie.dataanalysis.invertedindex;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Arrays;

/**
 * Created by pram on 16/06/2015.
 */
public class InvertedIndex {

    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length - 1),args[args.length - 1]);
    }

    public static void runJob(String[] input, String output) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Inverted Index Mapper");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        Path outputPath = new Path(output);

        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);

        outputPath.getFileSystem(conf).delete(outputPath, true);

        job.waitForCompletion(true);
    }
}
