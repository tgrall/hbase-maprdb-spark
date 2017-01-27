package com.mapr.sample.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

public class ReadFromHbaseDF {

  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf()
            .setAppName("ReadFromMapRDB-DF-Java")
            .setMaster("local[1]");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    SQLContext sqlContext = new SQLContext(jsc);

    Configuration config = null;
    try {
      config = HBaseConfiguration.create();
      config.set(TableInputFormat.INPUT_TABLE, "/apps/tests/users_profiles");
    } catch (Exception ce) {
      ce.printStackTrace();
    }

    JavaPairRDD hBaseRDD =
            jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

    // convert HBase result into Java RDD Pair key/User
    JavaPairRDD rowPairRDD = hBaseRDD.mapToPair(

            new PairFunction<Tuple2, String, User>() {
              @Override
              public Tuple2 call(
                      Tuple2 entry) throws Exception {

                Result r = (Result) entry._2;
                String rowKey = Bytes.toString(r.getRow());

                User user = new User();
                user.setRowkey( rowKey );
                user.setFirstName(Bytes.toString(r.getValue(Bytes.toBytes("default"), Bytes.toBytes("first_name"))));
                user.setLastName(Bytes.toString(r.getValue(Bytes.toBytes("default"), Bytes.toBytes("last_name"))));

                return new Tuple2(rowKey, user);
              }
            });

    System.out.println("************ RDD *************");
    System.out.println(rowPairRDD.count());
    System.out.println(rowPairRDD.keys().collect());
    System.out.println(rowPairRDD.values().collect());

    System.out.println("************ DF *************");
    DataFrame df = sqlContext.createDataFrame(rowPairRDD.values(), User.class);

    System.out.println(df.count());
    System.out.println(df.schema());
    df.show();

    System.out.println("************ DF with SQL *************");
    df.registerTempTable("USER_TABLE");
    DataFrame dfSql = sqlContext.sql("SELECT *  FROM USER_TABLE  WHERE firstName = 'Ally' ");
    System.out.println(dfSql.count());
    System.out.println(dfSql.schema());
    dfSql.show();


    jsc.close();

  }

}

