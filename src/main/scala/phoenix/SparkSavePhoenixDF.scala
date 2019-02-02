package phoenix

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.phoenix.spark._
import org.apache.hadoop.conf.Configuration
/**
  * 1. 创建表, 注意 info.companyname这里的info是列族名
  * CREATE TABLE test_company (areacode VARCHAR, code VARCHAR, info.companyname VARCHAR CONSTRAINT PK PRIMARY KEY (areacode, code));
  */
object SparkSavePhoenixDF {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("testPhonix")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val dataSet = List(("021", "54657", "Apple.com"), ("023", "64780", "hp.com"), ("010", "23567", "Google.com"))
    import sqlContext.implicits._
    val df = sc.parallelize(dataSet).map(x=>(x._1, x._2, x._3)).toDF("areacode", "code", "companyname")

    df.write.format("org.apache.phoenix.spark").mode(SaveMode.Overwrite).options( Map("table" -> "test_company",
      "zkUrl" -> "spark1234:12181")).save()

  }
}
