package offsetmanage

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._

/**
  * Created  on 下午9:37.
  * 将偏移量保存在ZK中
  * 1. 路径 //todo: labels is not supported
  * val zkPath: Nothing = s"${kakfaOffsetRootPath}/${groupName}/${o.topic}/${o.partition}"
  *2. 如果Zookeeper中未保存offset
  * 根据kafkaParam的配置使用最新或者最旧的offset 3.val zookeeper中有保存offset: Nothing = null
  * val 我们会利用这个offset作为kafkaStream: Nothing = null
  */
object KafkaZKManager extends Serializable {

  val client = {
    val client = CuratorFrameworkFactory
      .builder
      .connectString("spark123:12181/kafka0.9")
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .namespace("mykafka")
      .build()
    client.start()
    client
  }

  val kakfaOffsetRootPath = "/consumers/offsets"

  // 确保zookeeper中的路径是存在的
  def ensureZKPathExists(path: String): Unit = {
    if (client.checkExists().forPath(path) == null) {
      client.create().creatingParentsIfNeeded().forPath(path)
    }
  }

  def storeOffsets(offsetsRanges: Array[OffsetRange], groupName: String) = {


    for (o <- offsetsRanges) {
      // 保存offset到zk

    }

  }


  def getFromOffsets(topic: String, groupName: String): (Map[TopicAndPartition, Long], Int) = {
    // 如果 zookeeper中有保存offset,我们会利用这个offset作为kafkaStream 的起始位置
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    val zkTopicPath = s"${kakfaOffsetRootPath}/${groupName}/${topic}"
    ensureZKPathExists(zkTopicPath)
    val t = client.getChildren.forPath(zkTopicPath)
    val offsets = for {
      p <- client.getChildren.forPath(zkTopicPath)
    } yield {
      //遍历路径下面的partition中的offset
      val data = client.getData.forPath(s"$zkTopicPath/$p")
      //将data变成Long类型
      val offset = java.lang.Long.valueOf(new String(data)).toLong
      println("offset:" + offset)
      (TopicAndPartition(topic, Integer.parseInt(p)), offset)
    }

    if (offsets.isEmpty) {
      (offsets.toMap, 0)
    } else {
      (offsets.toMap, 1)
    }


  }


  def createMyDirectKafkaStream(ssc: StreamingContext, kafkaParams: Map[String, String], topic: String, groupName: String
                               ): InputDStream[(String, String)] = {
    val (fromOffsets, flag) = getFromOffsets(topic, groupName)

    var kafkaStream: InputDStream[(String, String)] = null
    if (flag == 1) {
      // 这个会将kafka的消息进行transform,最终kafak的数据都会变成(topic_name, message)这样的tuple
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      println("fromOffsets:" + fromOffsets)
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      // 如果未保存,根据kafkaParam的配置使用最新或者最旧的offset
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic.split(",").toSet)
    }
    kafkaStream
  }


  def main(args: Array[String]): Unit = {

    val processingInterval = 2
    val brokers = "spark123:9092"
    val topic = "mytest1"
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "smallest")


    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))


    val messages = createMyDirectKafkaStream(ssc, kafkaParams, topic, "testp")


    messages.foreachRDD((rdd, btime) => {
      if (!rdd.isEmpty()) {
        println("==========================:" + rdd.count())
        println("==========================btime:" + btime)
      }
      storeOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, "testp")
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
