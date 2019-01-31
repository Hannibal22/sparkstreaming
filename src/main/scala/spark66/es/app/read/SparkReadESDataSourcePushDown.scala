package spark66.es.app.read

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

/**
  * Created by hadoop on 18-3-3.
  */
object SparkReadESDataSourcePushDown {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    sparkConf.set("es.nodes","spark1234")
      .set("es.port","9200").set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)

    // sc = existing SparkContext
    val sqlContext = new SQLContext(sc)

    // options for Spark 1.4 - the path/resource is specified separately
    val options = Map("pushdown" -> "true", "es.nodes" -> "spark1234", "es.port" -> "9200")

    // Spark 1.4 style
    val spark14DF = sqlContext.read.format("org.elasticsearch.spark.sql")
      .options(options).load("company/info")

    //spark14DF.show()


    sqlContext.sql(
      "CREATE TEMPORARY TABLE myIndex    " +
        "USING org.elasticsearch.spark.sql " +
        "OPTIONS (resource 'company/info', scroll_size '20')" )
    sqlContext.sql("select * from myIndex").show



  }

}
