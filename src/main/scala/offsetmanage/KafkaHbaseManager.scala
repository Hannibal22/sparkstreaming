package kafkautils

import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.I0Itec.zkclient.ZkClient
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

import scala.collection.JavaConversions._

/**
  *
  * 将偏移量保存在hbase中
  *
  * 1. 基于Hbase的通用设计， 使用同一表保存可以跨越多个spark streaming程序的topic的offset 2. rowkey=topic名称+ groupid+ streaming的batchtime.milliSeconds.尽管
  *batchtime.milliSeconds不是必须的， 但是它可以看到历史的批处理任务对offset的管理情况。
  *3. kafka的offset保存在下面的表中，30天后自动过期 Hbase表结构
  * create 'spark_kafka_offsets', {NAME=>'offsets', TTL=>2592000}
  *4.offset的获取场景
  * 场景1:Streaming作业首次启动。 通过zookeeper来查找给定topic中分区的数量，然后返回“0” 作为所有topic分区的offset。
  * 场景2:长时间运行的Streaming作业已经停止，新的分区被添加到kafka的topic中。 通过 zookeeper来查找给定topic中分区的数量， 对于所有旧的topic分区，将offset设置为HBase中的 最新偏移量。 对于所有新的topic分区，它将返回“0”作为offset。
  * 场景3:长时间运行的Streaming作业已停止，topic分区没有任何更改。 在这种情况下，HBase
  * 中发现的最新偏移量作为每个topic分区的offset返回。
  *
  */
object KafkaHbaseManager {

  // 自己参考实现
  def saveOffsets(TOPIC_NAME: String, GROUP_ID: String, offsetRanges: Array[OffsetRange],
                  hbaseTableName: String, batchTime: org.apache.spark.streaming.Time) = {

  }


  // 从zookeeper中获取topic的分区数
  def getNumberOfPartitionsForTopicFromZK(TOPIC_NAME: String, GROUP_ID: String,
                                          zkQuorum: String, zkRootDir: String, sessTimeout: Int, connTimeOut: Int): Int = {

    return null;
  }

  // 自己参考实现
  def getLastestOffsets(TOPIC_NAME: String, GROUP_ID: String, hTableName: String,
                        zkQuorum: String, zkRootDir: String, sessTimeout: Int, connTimeOut: Int): Map[TopicAndPartition, Long] = {


    val zKNumberOfPartitions = getNumberOfPartitionsForTopicFromZK(TOPIC_NAME, GROUP_ID, zkQuorum, zkRootDir, sessTimeout, connTimeOut)


    val hbaseConf = HBaseConfiguration.create()

    // 获取hbase中最后提交的offset
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val table = conn.getTable(TableName.valueOf(hTableName))
    val startRow = TOPIC_NAME + ":" + GROUP_ID + ":" + String.valueOf(System.currentTimeMillis())
    val stopRow = TOPIC_NAME + ":" + GROUP_ID + ":" + 0
    val scan = new Scan()
    val scanner = table.getScanner(scan.setStartRow(startRow.getBytes).setStopRow(stopRow.getBytes).setReversed(true))
    val result = scanner.next()

    var hbaseNumberOfPartitions = 0 // 在hbase中获取的分区数量
    if (result != null) {
      // 将分区数量设置为hbase表的列数量
      hbaseNumberOfPartitions = result.listCells().size()
    }

    val fromOffsets = collection.mutable.Map[TopicAndPartition, Long]()
    if (hbaseNumberOfPartitions == 0) {
      // 初始化kafka为开始

    } else if (zKNumberOfPartitions > hbaseNumberOfPartitions) {
      // 处理新增加的分区添加到kafka的topic

    } else {
      // 获取上次运行的offset

    }

    scanner.close()
    conn.close()
    fromOffsets.toMap
  }

  def main(args: Array[String]): Unit = {
    // getLastCommittedOffsets("mytest1", "testp", "stream_kafka_offsets", "spark123:12181", "kafka0.9", 30000, 30000)

    val processingInterval = 2
    val brokers = "spark123:9092"
    val topics = "mytest1"
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("kafkahbase").setMaster("local[2]")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "smallest")


    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))
    val groupId = "testp"
    val hbaseTableName = "spark_kafka_offsets"

    // 获取kafkaStream
    //val kafkaStream = createMyDirectKafkaStream(ssc, kafkaParams, zkClient, topicsSet, "testp")
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    val fromOffsets = getLastestOffsets("mytest1", groupId, hbaseTableName, "spark123:12181", "kafka0.9", 30000, 30000)


    var kafkaStream: InputDStream[(String, String)] = null
    kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)


    kafkaStream.foreachRDD((rdd, btime) => {
      if (!rdd.isEmpty()) {
        println("==========================:" + rdd.count())
        println("==========================btime:" + btime)
        saveOffsets(topics, groupId, rdd.asInstanceOf[HasOffsetRanges].offsetRanges, hbaseTableName, btime)
      }

    })


    //val offsetsRanges:Array[OffsetRange] = null

    ssc.start()
    ssc.awaitTermination()


  }
}
