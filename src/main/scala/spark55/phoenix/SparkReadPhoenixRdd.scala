package spark55.phoenix

import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 18-2-19.
  */
object SparkReadPhoenixRdd {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("testPhonix")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)


    // Load the columns 'HOST' and 'DOMAIN' from WEB_STAT as an RDD
    val rdd: RDD[Map[String, AnyRef]] = sc.phoenixTableAsRDD(
      "WEB_STAT", Seq("HOST", "DOMAIN"), zkUrl = Some("spark1234:12181")
    )


    println(rdd.count())

    val host = rdd.first()("HOST").asInstanceOf[String]
    val domain = rdd.first()("DOMAIN").asInstanceOf[String]

    println("host:" + host)
    println("domain:" + domain)

    rdd.take(10).foreach(println)

  }
}
