package mapr.sample

import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.apache.spark._

object ReadFromHbaseDF {

  val cf = Bytes.toBytes("default");

  case class User(id: String, firstName : String, lastName : String);

  object User extends Serializable {

    def parseHbaseUserRow(result: Result): User = {
      val rowkey = Bytes.toString(result.getRow())
      val p0 = rowkey
      val p1 = Bytes.toString(result.getValue(cf, Bytes.toBytes("first_name")))
      val p2 = Bytes.toString(result.getValue(cf, Bytes.toBytes("last_name")))
      User(p0, p1, p2)
    }
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("ReadFromHbase");
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val conf = HBaseConfiguration.create()
    val tableName = "/apps/tests/users_profiles"

    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val resultRDD = hBaseRDD.map(tuple => tuple._2);

    val userRdd = resultRDD.map(User.parseHbaseUserRow);

    var userDF = userRdd.toDF();

    userDF.printSchema();

    userDF.show();

    userDF.registerTempTable("USER_HBASE");


    val resultSQl = sqlContext.sql("SELECT *  FROM USER_HBASE  WHERE firstName = 'Ally' ")

    resultSQl.show();


    sc.stop();
    println("=== END ===")

  }

}
