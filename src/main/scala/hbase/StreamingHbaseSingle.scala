package hbase

import utils.MyKafkaUtils.{createNewDirectKafkaStream, saveOffsets}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}
import ParseUtils

/**
  * 幂等写入( idempotent writes) rowkey设计 Kafka每条消息的partition、offset等获取
  * Hbase Rowkey: md5(topic + "|" + groupName + "|" + part) + String.format("%020d", java.lang.Long.valueOf(offset))
  */
object StreamingHbaseSingle {
  def main(args: Array[String]): Unit = {
    val processingInterval = 10
    val brokers = "spark1234:9092"

    // 创建kafka的topic： kafka-topics.sh --zookeeper localhost:12181/kafka0.9 --create --topic mykalog -partitions 3 --replication-factor 2
    val topic = "mykalog"
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    val topicsSet = topic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")

    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))
    val groupName = "hbaseGroup2"
    val hTableName = "mytest2"

    // 每条消息:  (topic, partition, offset, message)
    val streaming = createNewDirectKafkaStream(ssc, kafkaParams, Set(topic), groupName)
    streaming.foreachRDD(rdd => {

      // 方式一： 使用Hbase的批量API插入数据  （推荐）

      if (!rdd.isEmpty()) {
        rdd.map(x => ParseUtils.parseMsg(x, groupName)).foreachPartition(p => {
          val hbaseConf = HBaseConfiguration.create()
          hbaseConf.set("hbase.zookeeper.quorum", "spark1234")
          hbaseConf.set("hbase.zookeeper.property.clientPort", "12181")
          val conn = ConnectionFactory.createConnection(hbaseConf)
          val table = conn.getTable(TableName.valueOf(hTableName))
          import scala.collection.JavaConversions._
          table.put(seqAsJavaList(p.toSeq))
        })
      }



      /*
       // 方式二： 使用hadoop的文件系统api插入

      var jobConf = new JobConf(HBaseConfiguration.create)
      jobConf.set(TableOutputFormat.OUTPUT_TABLE, hTableName)
      jobConf.setOutputFormat(classOf[TableOutputFormat])

      if(!rdd.isEmpty()) {
        rdd.map(x => ParseUtils.parseToHadoopDataSet(x, groupName)).saveAsHadoopDataset(jobConf)
      }*/


      /*
      // 方式三： 使用hadoop的文件系统新api插入
      val hconf = HBaseConfiguration.create()
      val jobConf = new JobConf(hconf, this.getClass)
      jobConf.set(TableOutputFormat.OUTPUT_TABLE, hTableName)
      //设置job的输出格式
      val job = Job.getInstance(jobConf)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Result])
      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
      if(!rdd.isEmpty()) {
        rdd.map(x => ParseUtils.parseToHadoopDataSet(x, groupName)).saveAsNewAPIHadoopDataset(job.getConfiguration)
      }
       */



      saveOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, groupName)
    }
    )


    ssc.start()
    ssc.awaitTermination()


  }

}
