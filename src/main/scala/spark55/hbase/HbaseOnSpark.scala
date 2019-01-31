package spark55.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Hbase on Spark
  * 使用spark操作HBASE数据
  */
object HbaseOnSpark {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("testPhonix")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val df = convertToDF(sc, sqlContext, "mytest2", null)
    //println(df.count())
    df.filter("part='1'").show(false)
  }


  /**
    * desc: 将hbase表转换成DataFrame
    * @param sc
    * @param sqlContext
    * @param htable hbase表名
    * @return
    */
  def convertToDF(sc: SparkContext, sqlContext: SQLContext, htable: String, tuple2:Tuple2[String, String]): DataFrame = {
    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum","spark1234")
    //设置zookeeper连接端口，默认2181
    hBaseConf.set("hbase.zookeeper.property.clientPort", "12181")

 /*   hBaseConf.set(TableInputFormat.INPUT_TABLE, htable)
    if(tuple2 != null){
      hBaseConf.set(TableInputFormat.SCAN_ROW_START, tuple2._1)
      hBaseConf.set(TableInputFormat.SCAN_ROW_STOP, tuple2._2)
    }*/

    // 从数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    // 转换成DataFrame
    val resultDF = sqlContext.createDataFrame(hbaseRDD.map(row => parse(row)).filter(_.length!=1), struct)

    resultDF
  }


  // struct结构
  val struct = StructType(Array(
    StructField("rowkey", StringType),
    StructField("topic", StringType),
    StructField("part", StringType),
    StructField("offset", StringType),
    StructField("ifParseNormal", StringType),

    StructField("houseid", StringType),
    StructField("gathertime", StringType),
    StructField("srcip", StringType),
    StructField("destip", StringType),
    StructField("srcport", StringType),

    StructField("desport", StringType),
    StructField("url", StringType)
  ))


  /**
    * desc: 将hbase每条记录转换成Row
    * @param row  hbase每条记录的二元组, Tuple2[ImmutableBytesWritable, Result]
    * @return Row
    */
  def parse(row: Tuple2[ImmutableBytesWritable, Result]): Row = {
    try{
      val rkey = Bytes.toString(row._2.getRow)
      val topic = Bytes.toString(row._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("topic")))
      val part = Bytes.toString(row._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("part")))
      val offset = Bytes.toString(row._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("offset")))
      val ifParseNormal = Bytes.toString(row._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("ifParseNormal")))

      val houseid = Bytes.toString(row._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("houseid")))
      val gathertime = Bytes.toString(row._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("gathertime")))
      val srcip = Bytes.toString(row._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("srcip")))
      val destip = Bytes.toString(row._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("destip")))
      val srcport = Bytes.toString(row._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("srcport")))

      val desport = Bytes.toString(row._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("desport")))
      val url = Bytes.toString(row._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("url")))

      Row(
        rkey,
        if(null==topic) "-1" else topic,
        if(null==part) "-1" else part,
        if(null==offset) "-1" else offset,
        if(null==ifParseNormal) "-1" else ifParseNormal,

        if(null==houseid) "-1" else houseid,
        if(null==gathertime) "-1" else gathertime,
        if(null==srcip) "-1" else srcip,
        if(null==destip) "-1" else destip,
        if(null==srcport) "-1" else srcport,

        if(null==desport) "-1" else desport,
        if(null==url) "-1" else url
      )

    }catch {
      case e:Exception => {
        Row("0")
      }
    }

  }


}
