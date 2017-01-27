package com.mapr.sample.java;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class WriteToHbase {

  public static void main(String[] args) throws IOException {

    SparkConf sparkConf = new SparkConf()
            .setAppName("WriteToHbase-Java")
            .setMaster("local[1]");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    String tableName = "/apps/tests/users_profiles";


    List data = Arrays.asList("user-from-java-010,Jim,Tonic", "user-from-java-011,John,Davis", "user-from-java-012,Dave,Hill", "user-from-java-013,Ally,Deanon", "user-from-java-014,Joel,Dupont") ;
    JavaRDD rdd = jsc.parallelize(data);

    JavaRDD<User> userRdd = rdd.map(
            new Function<String, User>() {
              @Override
              public User call(String line) throws Exception {
                String[] userFields = line.split(",");
                User user = new User();
                user.setRowkey(userFields[0]);
                user.setFirstName(userFields[1]);
                user.setLastName( userFields[2]);
                return user;
              }
            }
    );

    Configuration hbaseConfiguration = null;
    try {
      hbaseConfiguration = HBaseConfiguration.create();
    } catch (Exception ce) {
      ce.printStackTrace();
    }


    Job job =  Job.getInstance(hbaseConfiguration);
    JobConf jobConf = (JobConf)job.getConfiguration();
    jobConf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp/");
    jobConf.setOutputFormat(TableOutputFormat.class);
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName );

    JavaPairRDD<ImmutableBytesWritable, Put> tablePuts = userRdd.mapToPair(
            new PairFunction<User, ImmutableBytesWritable, Put>() {
              @Override
              public Tuple2<ImmutableBytesWritable, Put> call(User user) throws Exception {
                Put put = new Put(Bytes.toBytes(user.getRowkey()));
                put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("first_name"), Bytes.toBytes(user.getFirstName()));
                put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("last_name"), Bytes.toBytes(user.getLastName()));
                return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
              }
            }
    );

    System.out.println("************* Saving Data Into Table ***************");
    tablePuts.saveAsHadoopDataset( jobConf );

    jsc.close();

  }

}
