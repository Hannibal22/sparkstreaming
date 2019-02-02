package sparkandes.es

import utils.MyKafkaUtils.{createNewDirectKafkaStream, saveOffsets}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import sparkandes.utils.ParseESUtils
import org.elasticsearch.spark.sql._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.elasticsearch.spark.streaming._

/**
  *
  * EasticSearch实现Exactly-once语义
  *
  * id设计 //todo: labels is not supported
  * val md5pre: Nothing = md5(topic + "|" + groupName + "|" + part)
  * val id: Nothing = md5pre + "|" + String.format("%020d", java.lang.Long.valueOf(offset))
  *
  * Spark Streming的foreachRDD方式
  *
  * 创建kafka的topic： kafka-topics.sh --zookeeper localhost:12181/kafka0.9 --create --topic mykalog -partitions 3 --replication-factor 2
  * kafka 生产数据: kafka-console-producer.sh --broker-list localhost:9092 --topic mykalog
  */
object StreamingESForEachRDD {
  def main(args: Array[String]): Unit = {
    val processingInterval = 10
    val brokers = "spark1234:9092"

    val topic = "mykalog"
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    sparkConf.set("es.nodes", "spark1234")
      .set("es.port", "9200").set("es.index.auto.create", "true")

    val esIndex = "myeslog1"
    val esType = "mydoc"

    val topicsSet = topic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")

    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))
    val groupName = "es2"

    // 每条消息:  (topic, partition, offset, message)
    val streaming = createNewDirectKafkaStream(ssc, kafkaParams, Set(topic), groupName)


    streaming.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val dataRow = rdd.map(x => ParseESUtils.parseMsg(x, offsetRanges, groupName))
        val struct = ParseESUtils.struct
        val sqlContext = new SQLContext(rdd.context)
        val dataDF = sqlContext.createDataFrame(dataRow, struct)
        dataDF.show(false)
        dataDF.saveToEs(s"${esIndex}/${esType}", Map("es.mapping.id" -> "id"))
        saveOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, groupName)
      }
    }
    )

    ssc.start()
    ssc.awaitTermination()


  }

}
