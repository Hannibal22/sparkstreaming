package sparkandes.es.app.write

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by hadoop on 18-3-3.
  */
object SparkWrite2ESCase {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    sparkConf.set("es.nodes","spark1234")
      .set("es.port","9200").set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)

    // define a case class
    case class Trip(deptid: String, departure: String, arrival: String)

    val upcomingTrip = Trip("d001", "OTP", "SFO")
    val lastWeekTrip = Trip("d002", "MUC", "OTP")

    val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))

    //EsSpark.saveToEs(rdd, "spark1/docs")
    // 指定id
    EsSpark.saveToEs(rdd, "spark2/docs",Map("es.mapping.id" -> "deptid"))


  }

}
