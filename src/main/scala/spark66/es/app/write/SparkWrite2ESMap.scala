package spark66.es.app.write

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * Created by hadoop on 18-3-3.
  */
object SparkWrite2ESMap {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    // 或者去掉spark.前缀， 但是通过spark-shell 或者spark-submit设置属性必须要以spark.为前缀
    sparkConf.set("spark.es.nodes","spark1234")
      .set("spark.es.port","9200").set("spark.es.index.auto.create", "true")
    //sparkConf.set("es.nodes","spark1234")
    //  .set("es.port","9200").set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)

    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")



  }

}
