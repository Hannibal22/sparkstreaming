package spark66.es

import kafka4.utils.MyKafkaUtils.{createNewDirectKafkaStream, saveOffsets}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark66.utils.ParseESUtils
import org.elasticsearch.spark.sql._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.elasticsearch.spark.streaming._

/**
  * Created by hadoop on 18-2-21.
  *
  */
object StreamingESForDstream {
  def main(args: Array[String]): Unit = {
    val processingInterval = 10
    val brokers = "spark1234:9092"

    // 创建kafka的topic： kafka-topics.sh --zookeeper localhost:12181/kafka0.9 --create --topic mykalog -partitions 3 --replication-factor 2
    val topic = "mykalog"
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    sparkConf.set("es.nodes","spark1234")
      .set("es.port","9200").set("es.index.auto.create", "true")

    val esIndex = "myeslog2"
    val esType = "mydoc"

    val topicsSet = topic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")

    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))
    val groupName = "es2"

    // 每条消息:  (topic, partition, offset, message)
    val streaming = createNewDirectKafkaStream(ssc, kafkaParams, Set(topic), groupName)


 /*   case class user1(topic:String, part:Int, offset:Long)

    streaming.map(x=>user1(x._1, x._2, x._3)).saveToEs("test/mydoc")*/


    val dstream = streaming.transform(rdd=>{
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val newrdd = rdd.map(x=>(x._1, x._2, x._3, x._4, offsetRanges))
      newrdd
    })

    val newDstream =dstream.map(rdd=> {
      val offsetRanges = rdd._5
      val dataMap = ParseESUtils.parseMsgtoMap((rdd._1, rdd._2, rdd._3, rdd._4), offsetRanges, groupName)
      dataMap
    }
    )
    EsSparkStreaming.saveToEs(newDstream, s"${esIndex}/${esType}", Map("es.mapping.id"->"id"))

    streaming.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){
        saveOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, groupName)
      }
    }

    )


    ssc.start()
    ssc.awaitTermination()


  }

}
