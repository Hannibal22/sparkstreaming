package spark66.es.app.write

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.Metadata._
/**
  * Created by hadoop on 18-3-3.
  */
object SparkWrite2ESMeta {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    sparkConf.set("es.nodes","spark1234")
      .set("es.port","9200").set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)

    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    /*
    // 处理文档元数据: 手动指定文档ID
    val airportsRDD = sc.makeRDD(Seq((1, otp), (2, muc), (3, sfo)))
    airportsRDD.saveToEsWithMeta("airports/2015")
    */

    // metadata for each document
    // note it's not required for them to have the same structure
    // 可设置的元数据属性： ID,PARENT,ROUTING,TTL,TIMESTAMP,VERSION,VERSION_TYPE
    val otpMeta = Map(ID -> 1, TTL -> "3h")
    val mucMeta = Map(ID -> 2, VERSION -> "23")
    val sfoMeta = Map(ID -> 3)
    val airportsRDD = sc.makeRDD(Seq((otpMeta, otp), (mucMeta, muc), (sfoMeta, sfo)))
    airportsRDD.saveToEsWithMeta("airports2/2015")
  }

}
