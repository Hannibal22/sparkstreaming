package spark66.es.app.read

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * Created by hadoop on 18-3-3.
  */
object SparkReadESJson {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    sparkConf.set("es.nodes","spark1234")
      .set("es.port","9200").set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)

    val rdd = sc.esJsonRDD("airports/2015")
    //val rdd = sc.esJsonRDD("spark/docs", "?q=SFO*")

    rdd.take(10).foreach(println)

  }

}
