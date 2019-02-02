package phoenix

import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 18-2-19.
  */
object SparkReadPhoenixTable {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("testPhonix")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val conf = new Configuration()
    conf.set("hbase.zookeeper.quorum","spark1234:12181")

    val df = sqlContext.phoenixTableAsDataFrame(
      "WEB_STAT", Array("HOST", "DOMAIN"),conf = conf
    )


    df.filter(df("HOST") === "EU").show

  }
}
