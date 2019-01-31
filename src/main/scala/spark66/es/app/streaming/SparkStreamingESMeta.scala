package spark66.es.app.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.elasticsearch.spark.streaming._
import org.elasticsearch.spark.rdd.Metadata._


import scala.collection.mutable


/**
  * Created by hadoop on 18-3-3.
  * 注意： 需要先创建好index : curl -XPUT http://spark1234:9200/sparkmeta
  */
object SparkStreamingESMeta {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    sparkConf.set("es.nodes","spark1234")
      .set("es.port","9200").set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(10))

    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    /*
    //  方式一： 手动指定文档ID
    val airportsRDD = sc.makeRDD(Seq((1, otp), (2, muc), (3, sfo)))
    val microbatches = mutable.Queue(airportsRDD)

    ssc.queueStream(microbatches).saveToEsWithMeta("streaming-meta1/2015")

    */

    //  方式二： 指定更多属性
    // metadata for each document
    // note it's not required for them to have the same structure
    val otpMeta = Map(ID -> 1, TTL -> "3h")
    val mucMeta = Map(ID -> 2, VERSION -> "23")
    val sfoMeta = Map(ID -> 3)
    val airportsRDD = sc.makeRDD(Seq((otpMeta, otp), (mucMeta, muc), (sfoMeta, sfo)))
    val microbatches = mutable.Queue(airportsRDD)

    ssc.queueStream(microbatches).saveToEsWithMeta("streaming-meta2/2015")


    ssc.start()

    ssc.awaitTermination()
  }

}
