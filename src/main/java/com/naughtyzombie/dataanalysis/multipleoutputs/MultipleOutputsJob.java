package com.naughtyzombie.dataanalysis.multipleoutputs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public final class MultipleOutputsJob extends Configured implements Tool {

  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MultipleOutputsJob(), args);
    System.exit(res);
  }

  public int run(final String[] args) throws Exception {

    Path input = new Path(args[0]);
    Path output = new Path(args[1]);

    Configuration conf = super.getConf();

    Job job = new Job(conf);
    job.setJarByClass(MultipleOutputsJob.class);
    job.setMapperClass(MultipleOutputMapper.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    job.setNumReduceTasks(0);

    MultipleOutputs.addNamedOutput(job, "partition", TextOutputFormat.class, Text.class, Text.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }  
}