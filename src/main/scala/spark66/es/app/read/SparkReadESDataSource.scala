package spark66.es.app.read

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

/**
  * Created by hadoop on 18-3-3.
  */
object SparkReadESDataSource {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    sparkConf.set("es.nodes","spark1234")
      .set("es.port","9200").set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)

    // sc = existing SparkContext
    val sqlContext = new SQLContext(sc)


    // spark 1.3
   // val df = sqlContext.load("company/info", "org.elasticsearch.spark.sql")

    // spark 1.4
   // val df = sqlContext.read.format("org.elasticsearch.spark.sql").load("company/info")

    // spark 1.5+
    val df = sqlContext.read.format("es").load("company/info")
    df.show()

  }

}
