package sparkandes.es.app.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.elasticsearch.spark.streaming._

import scala.collection.mutable


/**
  * Created by hadoop on 18-3-3.
  */
object SparkStreamingESJson {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    sparkConf.set("es.nodes","spark1234")
      .set("es.port","9200").set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(5))

    val json1 = """{"reason" : "business", "airport" : "SFO"}"""
    val json2 = """{"participants" : 5, "airport" : "OTP"}"""


    val rdd = sc.makeRDD(Seq(json1, json2))
    val microbatch = mutable.Queue(rdd)
    ssc.queueStream(microbatch).saveJsonToEs("streamingjson/json-trips")

    ssc.start()
    ssc.awaitTermination()

  }

}
