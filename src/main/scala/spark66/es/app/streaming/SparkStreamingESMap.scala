package spark66.es.app.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.elasticsearch.spark.streaming._

import scala.collection.mutable


/**
  * Created by hadoop on 18-3-3.
  */
object SparkStreamingESMap {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    sparkConf.set("es.nodes","spark1234")
      .set("es.port","9200").set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(10))

    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    val rdd = sc.makeRDD(Seq(numbers, airports))
    val microbatches = mutable.Queue(rdd)

    ssc.queueStream(microbatches).saveToEs("sparkstreaming/docs")

    ssc.start()
    ssc.awaitTermination()

  }

}
