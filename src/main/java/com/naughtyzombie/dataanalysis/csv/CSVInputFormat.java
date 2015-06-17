package com.naughtyzombie.dataanalysis.csv;

import com.naughtyzombie.dataanalysis.utils.CSVParser;
import com.naughtyzombie.dataanalysis.utils.TextArrayWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class CSVInputFormat extends
        FileInputFormat<LongWritable, TextArrayWritable> {

    public static String CSV_TOKEN_SEPARATOR_CONFIG =
            "csvinputformat.token.delimiter";

    @Override
    public RecordReader<LongWritable, TextArrayWritable>
    createRecordReader(InputSplit split,
                       TaskAttemptContext context) {
        String csvDelimiter = context.getConfiguration().get(CSV_TOKEN_SEPARATOR_CONFIG);

        Character separator = null;
        if (csvDelimiter != null && csvDelimiter.length() == 1) {
            separator = csvDelimiter.charAt(0);
        }

        return new CSVRecordReader(separator);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration())
                        .getCodec(file);
        return codec == null;
    }

    public static class CSVRecordReader
            extends RecordReader<LongWritable, TextArrayWritable> {
        private LineRecordReader reader;
        private TextArrayWritable value;
        private final CSVParser parser;

        public CSVRecordReader(Character csvDelimiter) {
            this.reader = new LineRecordReader();
            if (csvDelimiter == null) {
                parser = new CSVParser();
            } else {
                parser = new CSVParser(csvDelimiter);
            }
        }

        @Override
        public void initialize(InputSplit split,
                               TaskAttemptContext context)
                throws IOException, InterruptedException {
            reader.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue()
                throws IOException, InterruptedException {
            if (reader.nextKeyValue()) {
                loadCSV();
                return true;
            } else {
                value = null;
                return false;
            }
        }

        private void loadCSV() throws IOException {
            String line = reader.getCurrentValue().toString();
            String[] tokens = parser.parseLine(line);
            value = new TextArrayWritable(convert(tokens));
        }

        private Text[] convert(String[] s) {
            Text t[] = new Text[s.length];
            for (int i = 0; i < t.length; i++) {
                t[i] = new Text(s[i]);
            }
            return t;
        }

        @Override
        public LongWritable getCurrentKey()
                throws IOException, InterruptedException {
            return reader.getCurrentKey();
        }

        @Override
        public TextArrayWritable getCurrentValue()
                throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress()
                throws IOException, InterruptedException {
            return reader.getProgress();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}