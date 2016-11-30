package mapr.sample

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkContext, SparkConf}

object ReadFromHbaseWithRowkey {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("ReadFromHbaseWithRowkey");
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val tableName = "/apps/tests/users_profiles"

    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN_ROW_START, "user-000");
    conf.set(TableInputFormat.SCAN_ROW_STOP, "user-012");
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    println("Number of rows : " + hBaseRDD.count())
    sc.stop()
  }

}
