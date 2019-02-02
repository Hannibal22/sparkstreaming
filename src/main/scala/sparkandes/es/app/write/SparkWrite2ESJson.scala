package sparkandes.es.app.write

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * Created by hadoop on 18-3-3.
  */
object SparkWrite2ESJson {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    sparkConf.set("es.nodes","spark1234")
      .set("es.port","9200").set("es.index.auto.create", "true")


    val json1 = """{"reason" : "business", "airport" : "SFO"}"""
    val json2 = """{"participants" : 5, "airport" : "OTP"}"""

    new SparkContext(sparkConf).makeRDD(Seq(json1, json2))
      .saveJsonToEs("sparkjson/json-trips")



  }

}
