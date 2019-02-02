package sparkandes.es.app.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.elasticsearch.spark.streaming._

import scala.collection.mutable


/**
  * Created by hadoop on 18-3-3.
  * 注意： 需要先创建好index : curl -XPUT http://spark1234:9200/sparkmeta
  */
object SparkStreamingESDynamic {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    sparkConf.set("es.nodes","spark1234")
      .set("es.port","9200").set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(10))

    val game = Map("media_type"->"game","title" -> "FF VI","year" -> "1994")
    val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
    val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")

    val batch = sc.makeRDD(Seq(game, book, cd))
    val microbatches = mutable.Queue(batch)
    ssc.queueStream(microbatches).saveToEs("streaming-collection/{media_type}")

    ssc.start()
    ssc.awaitTermination()
  }

}
