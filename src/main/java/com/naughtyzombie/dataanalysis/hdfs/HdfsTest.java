package com.naughtyzombie.dataanalysis.hdfs;

import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

public class HdfsTest {

    public static void main(String args[]) {

        try {
            UserGroupInformation ugi
                = UserGroupInformation.createRemoteUser("guest");

            ugi.doAs(new PrivilegedExceptionAction<Void>() {

                public Void run() throws Exception {

                    Configuration conf = new Configuration();
                    conf.set("fs.defaultFS", "hdfs://PramithtalesPro:8020");
                    conf.set("hadoop.job.ugi", "guest");

                    FileSystem fs = FileSystem.get(conf);

                    fs.createNewFile(new Path("/user/guest/test"));

                    FileStatus[] status = fs.listStatus(new Path("/user/guest"));
                    for(int i=0;i<status.length;i++){
                        System.out.println(status[i].getPath());
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}