package com.naughtyzombie.dataanalysis.invertedindex;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class IndexReducer extends Reducer<Text, Text, Text, Text> {

    private Text docIds = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Set<Text> uniqueDocIds = new HashSet<Text>();
        for (Text docId : values) {
            uniqueDocIds.add(new Text(docId));
        }
        docIds.set(new Text(StringUtils.join(uniqueDocIds, ",")));
        context.write(key, docIds);
    }
}