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