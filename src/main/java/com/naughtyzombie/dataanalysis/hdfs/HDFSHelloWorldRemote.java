package com.naughtyzombie.dataanalysis.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


public class HDFSHelloWorldRemote {

  public static final String theFilename = "hello.txt";
  public static final String message = "Hello, world!\n";

  public static void main (String [] args) throws IOException {

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://192.168.0.10:8020");
    FileSystem fs = FileSystem.get(conf);

    Path filenamePath = new Path(theFilename);

    try {
      if (fs.exists(filenamePath)) {
        // remove the file first
        fs.delete(filenamePath,true);
      }

      FSDataOutputStream out = fs.create(filenamePath);
      out.writeUTF(message);
      out.close();

      FSDataInputStream in = fs.open(filenamePath);
      String messageIn = in.readUTF();
      System.out.print(messageIn);
      in.close();
    } catch (IOException ioe) {
      System.err.println("IOException during operation: " + ioe.toString());
      System.exit(1);
    }
  }
}