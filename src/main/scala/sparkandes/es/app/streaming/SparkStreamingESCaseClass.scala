package sparkandes.es.app.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.elasticsearch.spark.streaming.EsSparkStreaming
import scala.collection.mutable


/**
  * Created by hadoop on 18-3-3.
  * 注意： 需要先创建好index : curl -XPUT http://spark1234:9200/sparkcase
  */
object SparkStreamingESCaseClass {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    sparkConf.set("es.nodes","spark1234")
      .set("es.port","9200").set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(10))

    // define a case class
    case class Trip(deptid: String, departure: String, arrival: String)

    val upcomingTrip = Trip("d001", "OTP", "SFO")
    val lastWeekTrip = Trip("d002","MUC", "OTP")

    val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
    val microbatches = mutable.Queue(rdd)
    val dstream = ssc.queueStream(microbatches)

    // EsSparkStreaming.saveToEs(dstream, "sparkcase/docs")

    // 指定元数据字段
    EsSparkStreaming.saveToEs(dstream, "sparkmeta/docs", Map("es.mapping.id"->"deptid"))


    ssc.start()
    ssc.awaitTermination()
  }

}
