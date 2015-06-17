package com.naughtyzombie.dataanalysis.multipleoutputs;

import com.naughtyzombie.dataanalysis.stocks.StockPriceWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class MultipleOutputMapper extends Mapper<LongWritable, Text, Text, Text> {

    private MultipleOutputs output;

    @Override
    protected void setup(Context context) {
        output = new MultipleOutputs(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StockPriceWritable stock = StockPriceWritable.fromLine(value.toString());

        output.write(value, null, stock.getDate());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        output.close();
    }
}