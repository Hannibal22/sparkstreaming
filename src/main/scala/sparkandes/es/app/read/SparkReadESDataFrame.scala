package sparkandes.es.app.read

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

/**
  * Created by hadoop on 18-3-3.
  */
object SparkReadESDataFrame {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    sparkConf.set("es.nodes","spark1234")
      .set("es.port","9200").set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)

    // sc = existing SparkContext
    val sqlContext = new SQLContext(sc)


    //val company = sqlContext.esDF("company/info")

    val company = sqlContext.esDF("company/info", "?q=023", Map("es.read.field.include" -> "companyname") )

    // check the associated schema
    println(company.schema.treeString)
   // root
   // |-- areacode: string (nullable = true)
   // |-- code: string (nullable = true)
   // |-- companyname: string (nullable = true)

    company.show

  }

}
