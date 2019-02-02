package semantic

import java.sql.DriverManager

import utils.MyKafkaUtils._
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

/**
  *
  * 消息传递语义(Exactly-once)
  *
  * 幂等写入( idempotent writes)
  * 因为重复消费数据写入会被覆盖，所以可以确保Exactly-once
  */
object KafkaOffsetIdempotent {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    val processingInterval = 2
    val brokers = "spark1234:9092"
    val topic = "mytopic1"
    // Create direct kafka stream with brokers and topics
    val topicsSet = topic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,  "auto.offset.reset" -> "smallest")


    /*

       1. 创建测试的mysql数据库
       create database mytest;
       2. 建表
       create table myorders(name, orderid varchar(100) primary key);
       3. 新建topic： mytopic1
         kafka-topics.sh --zookeeper localhost:12181/kafka0.9 --create --topic mytopic1 --partitions 3 --replication-factor 1
       4. 往mytopic1发送数据， 数据格式为 "字符,数字"  比如  abc,3
     */

    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))


    val groupName =  "myspark"
    val messages = createMyDirectKafkaStream(ssc, kafkaParams, topicsSet, groupName)

    val jdbcUrl =  "jdbc:mysql://localhost:3306/mytest"
    val jdbcUser = "root"
    val jdbcPassword = "123456"

    messages.foreachRDD(rdd=>{
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.map(x=>x._2).foreachPartition(partition =>{

        val pOffsetRange = offsetRanges(TaskContext.get.partitionId)


        val dbConn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
       // dbConn.setAutoCommit(false)



        // upsert update insert
        partition.foreach(msg=>{
          val name = msg.split(",")(0)
          val orderid = msg.split(",")(1)
          val sql = s"insert into myorders(name, orderid) values ('$name', '$orderid') ON DUPLICATE KEY UPDATE name='${name}'"
          val pstmt = dbConn.prepareStatement(sql)
          pstmt.execute()
        })

        // dbConn.commit()
        dbConn.close()
      })
      saveOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, groupName)
    })


    ssc.start()
    ssc.awaitTermination()
  }

}
