package kafka4.semantic

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}
import scalikejdbc.{ConnectionPool, DB}
import scalikejdbc._


/**
  * 事务{
  *     保存数据
  *     保存offset
  * }
  *
  *
  * 1. 创建测试的mysql数据库
       create database mytest;
    2. 新建topic： mytopic1
       kafka-topics.sh --zookeeper localhost:12181/kafka0.9 --create --topic mytopic1 --partitions 3 --replication-factor 1
    3. 建表
       create table mytopic(topic varchar(200), partid int, offset bigint);
       create table mydata(name varchar(200), id int);

       初始化表：
        insert into mytopic(topic, partid, offset) values('mytopic1',0,0);
        insert into mytopic(topic, partid, offset) values('mytopic1',1,0);
        insert into mytopic(topic, partid, offset) values('mytopic1',2,0);
    4. 往mytopic1发送数据， 数据格式为 "字符,数字"  比如  abc,3

    5. 在pom文件加入依赖
       <dependency>
          <groupId>org.scalikejdbc</groupId>
          <artifactId>scalikejdbc_2.10</artifactId>
          <version>2.2.1</version>
      </dependency>

  */
object KafkaOffsetTransanction {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    val processingInterval = 2
    val brokers = "spark1234:9092"
    val topic = "mytopic1"
    // Create direct kafka stream with brokers and topics
    val topicsSet = topic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,  "auto.offset.reset" -> "smallest")


    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))


    val groupName =  "myspark"
    //val messages = createMyDirectKafkaStream(ssc, kafkaParams, topicsSet, groupName)



    val driver = "com.mysql.jdbc.Driver"
    val jdbcUrl =  "jdbc:mysql://localhost:3306/mytest"
    val jdbcUser = "root"
    val jdbcPassword = "123456"

    // 设置jdbc
    Class.forName(driver)
    // 设置连接池
    ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPassword)
    val fromOffsets = DB.readOnly { implicit session => sql"select topic, partid, offset from mytopic".
        map { r =>
          TopicAndPartition(r.string(1), r.int(2)) -> r.long(3)
        }.list.apply().toMap
    }

    val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    val  messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)



    messages.foreachRDD(rdd=> {
      rdd.foreachPartition(partiton=>{

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val pOffsetRange = offsetRanges(TaskContext.get.partitionId)


        // localTx
        DB.localTx { implicit session =>

          partiton.foreach(msg=>{
            // 或者使用scalike的batch 插入
            val name = msg._2.split(",")(0)
            val id =msg._2.split(",")(1)
            val dataResult = sql"""insert into  mydata(name,id) values (${name},${id})""".execute().apply()

          })

          val offsetResult =
            sql"""update mytopic set offset = ${pOffsetRange.untilOffset} where topic =
                  ${pOffsetRange.topic} and partid = ${pOffsetRange.partition}""".update.apply()

        }
      })
    }
    )



    ssc.start()
    ssc.awaitTermination()
  }

}
