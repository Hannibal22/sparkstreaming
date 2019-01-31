package spark66.es.app.read

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{MapWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.elasticsearch.spark._

/**
  * Created by hadoop on 18-3-3.
  */
object SparkReadHadoopNewAPI {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    sparkConf.set("spark.serializer",
      classOf[KryoSerializer].getName)

    val sc = new SparkContext(sparkConf)

    val conf = new Configuration()
    conf.set("es.resource", "company/info")
    //conf.set("es.query", "?q=me*")
    val esRDD = sc.newAPIHadoopRDD(conf,
      classOf[EsInputFormat[Text, MapWritable]],
      classOf[Text], classOf[MapWritable])

    val docCount = esRDD.count();
    println("docCount:" + docCount)
    esRDD.take(10).foreach(println)

  }

}
