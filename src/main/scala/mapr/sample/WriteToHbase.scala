package mapr.sample

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkContext, SparkConf}


object WriteToHbase {

  case class User(id: String, firstName : String, lastName : String);


  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("ReadFromHbase");
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val tableName = "/apps/tests/users_profiles"



    val data = Array("user-010,Jim,Tonic", "user-011,John,Davis", "user-012,Dave,Hill", "user-013,Ally,Deanon", "user-014,Joel,Dupont");
    val rdd = sc.parallelize(data);

    val userRdd = rdd.map(_.split(",")).map(p => User(p(0), p(1), p(2)));

    userRdd.foreach(println);

    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/tmp/")
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)


    userRdd.map { case (k) => convertToPut(k) }.saveAsHadoopDataset(jobConfig)

    println("=== END ===");
    sc.stop();

  }


  def convertToPut(user : User): (ImmutableBytesWritable, Put) = {
    val p = new Put(Bytes.toBytes( user.id ))
    // add columns with data values to put
    p.add(Bytes.toBytes("default"), Bytes.toBytes("first_name"), Bytes.toBytes( user.firstName ))
    p.add(Bytes.toBytes("default"), Bytes.toBytes("last_name"), Bytes.toBytes( user.lastName))
    (new ImmutableBytesWritable, p)
  }


}
