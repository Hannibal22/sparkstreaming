package spark55.phoenix

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.phoenix.spark._

/**
  * Created by hadoop on 18-2-19.
  * 步骤：
  *     1. 创建表
  *        CREATE TABLE OUTPUT_TEST_TABLE (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR, col2 INTEGER);
        2. 创建rdd， 保存
  */
object SparkSavePhoenixRDD {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("testPhonix")
    val sc = new SparkContext(sparkConf)

    val dataSet = List((1, "1", 1), (2, "2", 2), (3, "3", 3))

    sc.parallelize(dataSet).saveToPhoenix("OUTPUT_TEST_TABLE", Seq("ID","COL1","COL2"), zkUrl = Some("spark1234:12181"))


  }
}
