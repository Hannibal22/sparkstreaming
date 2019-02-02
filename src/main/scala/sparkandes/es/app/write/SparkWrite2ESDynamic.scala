package sparkandes.es.app.write

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * Created by hadoop on 18-3-3.
  */
object SparkWrite2ESDynamic {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    sparkConf.set("es.nodes","spark1234")
      .set("es.port","9200").set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)

    val game = Map("media_type"->"game","title" -> "FF VI","year" -> "1994")
    val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
    val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")

    sc.makeRDD(Seq(game, book, cd)).saveToEs("my-collection/{media_type}")



  }

}
