# sparkstreaming
1.
wordcount.DirectKafkaWordCount ：使用直连的方式，做一个workcount Demo
wordcount.ReceiveKafkaWordCount ：使用receive的方式，做一个workcount Demo
wordcount.KafkaWordCountProducer：使用kafka，生产一些数据用于测试

2.偏移量的管理：
offsetmanage.StreamingWithCheckpoint ：启用Spark Streaming的checkpoint是存储偏移量最简单的方法
offsetmanage.KafkaZKManager ：将偏移量保存在ZK中
kafkautils.KafkaHbaseManager：将偏移量保存在hbase中

3.消息传递语义(Exactly-once)
semantic.KafkaOffsetIdempotent ：幂等写入( idempotent writes)，因为重复消费数据写入会被覆盖，所以可以确保Exactly-once
semantic.KafkaOffsetTransanction ：事务控制，保存数据和offset在同一个事务里面
semantic.KafkaOffsetApp ：自己实现Exactly-once，offset和数据绑定保存等
sparkandes.es.StreamingESForEachRDD ：EasticSearch实现Exactly-once语义
sparkandes.es.StreamingESForDstream ：EasticSearch实现Exactly-once语义
hbase.StreamingHbaseSingle：sparkStreaming实时消费数据，幂等写入hbase

4.
utils.MyKafkaUtils ：kafka工具类，解决kafka的保存的offset过期问题；createMyDirectKafkaStream创建连接，与保存偏移量等功能

5.
kafka2kafka.MyKafkaProducer：作用就是，将KafkaProducer对象广播到所有的executor节点， 这样就可以在每个executor节点将数据插入到kafka

6.
hbase.HbaseOnSpark：使用spark操作HBASE数据
sparkandes.es.app ：spark与sparkstreaming操作es(读，写)

7.
kafka2kafka.Kafka2KafkaPerBatch：Spark Streaming消费数据反写Kafka， 按照batch的方式
kafka2kafka.Kafka2KafkaPerPartition：Spark Streaming消费数据反写Kafka，按照partition的方式,可以提高效率

8.
kafka2kafka.KafkaOffsetApp：验证kafka的offset越界
kafka2kafka.KafkaRate：数据峰值期间如何限速？？限速参数：spark.streaming.kafka.maxRatePerPartition  每秒每个分区的记录数

9.
phoenix :spark整合phoenix

10.
mergefile.FilesManage ：合并小文件
