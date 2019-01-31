package spark66.es.app.write

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

/**
  * Created by hadoop on 18-3-3.
  */
object SparkWrite2ESDataFrame {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    sparkConf.set("es.nodes","spark1234")
      .set("es.port","9200").set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)

    // sc = existing SparkContext
    val sqlContext = new SQLContext(sc)

    //  create DataFrame
    val dataSet = List(("021", "54657", "Apple.com"), ("023", "64780", "hp.com"), ("010", "23567", "Google.com"))
    import sqlContext.implicits._
    val df = sc.parallelize(dataSet).map(x=>(x._1, x._2, x._3)).toDF("areacode", "code", "companyname")

    df.saveToEs("company/info")
  }

}
