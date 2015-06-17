# Data Analysis
Big Data Analysis examples using Hadoop

After cloning run the following command from the checkout directory

    chmod 755 gradlew
    
then run

    ./gradlew
    
and make sure that all the dependencies are pulled down. Assuming that runs you can now build the artifact using the command

    ./gradlew assemble
    
If you now check the directory `build/libs/` you should see the jar file present.  

You now want to copy the data into hdfs. Run the command

    hadoop fs -copyFromLocal data
    
Now you can run the job. For example running the following

    hadoop jar build/libs/data-analysis.jar com.naughtyzombie.dataanalysis.mapreduce.AggregateJob data output
    
The job should and complete. Check the output folder in hdfs to ensure that the results are present.

    hadoop fs -ls output
    
Running the LogWritable example

    hadoop jar build/libs/data-analysis.jar com.naughtyzombie.dataanalysis.logwritable.LogProcessor /user/guest/logwritable-input /user/guest/logwritable-output 3
    
Running the Analytics examples

    hadoop jar build/libs/data-analysis.jar com.naughtyzombie.dataanalysis.analytics.weblog.MsgSizeAggregateMapReduce /user/guest/logwritable-input /user/guest/msgsize-out
    hadoop jar build/libs/data-analysis.jar com.naughtyzombie.dataanalysis.analytics.weblog.HitCountMapReduce /user/guest/logwritable-input /user/guest/hit-count-out
    hadoop jar build/libs/data-analysis.jar com.naughtyzombie.dataanalysis.analytics.weblog.FrequencyDistributionMapReduce /user/guest/hit-count-out /user/guest/freq-dist-out
    hadoop jar build/libs/data-analysis.jar com.naughtyzombie.dataanalysis.analytics.mbox.CountReceivedRepliesMapReduce /user/guest/mbox /user/guest/count-replies-out
    hadoop jar build/libs/data-analysis.jar com.naughtyzombie.dataanalysis.analytics.mbox.CountSentRepliesMapReduce /user/guest/mbox /user/guest/count-emails-out
    hadoop jar build/libs/data-analysis.jar com.naughtyzombie.dataanalysis.analytics.mbox.JoinSentReceivedReplies /user/guest/join-input /user/guest/join-output
    
Running the Inverted Index Example

    hdfs dfs -mkdir -p invertedindex/input
    echo "cat sat mat" | hdfs dfs -put - invertedindex/input/1.txt
    echo "dog lay mat" | hdfs dfs -put - invertedindex/input/2.txt
    hadoop jar build/libs/data-analysis.jar com.naughtyzombie.dataanalysis.invertedindex.InvertedIndex invertedindex/input invertedindex/output
    
    hdfs dfs -cat invertedindex/output/part*
    
Running the CSV example. Upload an example CSV file

    hadoop jar build/libs/data-analysis.jar com.naughtyzombie.dataanalysis.csv.CSVMapReduce csv/input csv/output
    
    hdfs dfs -cat csv/output/part*
    
## Config notes

The VM config area is `/etc/hadoop/conf/`

[DFS Health](http://192.168.0.10:50070/dfshealth.jsp) Monitoring App

