package spark55.utils

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import java.io.UnsupportedEncodingException
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException

import org.apache.hadoop.hbase.io.ImmutableBytesWritable


/**
  *  解析日志
  */
object ParseUtils {

  /**
    * @param line
    * @return
    */
  def parseMsg(line:Tuple4[String, Int, Long, String], groupName:String) :Put = {
    val topic = line._1
    val part = line._2
    val offset = line._3

    try{
      val msg = line._4.replaceAll("<<<!>>>", "")
      val arr = msg.split(",", 15)
      val houseid= arr(1)
      val gathertime = arr(2)
      val srcip = arr(3)
      val destip = arr(4)
      val srcport = arr(5)
      val desport = arr(6)
      val url = arr(13)
      val md5pre = md5(topic + "|" + groupName + "|" + part)

      val rowkey = md5pre + "|" + String.format("%020d", java.lang.Long.valueOf(offset))
      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("topic"), Bytes.toBytes(topic))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("part"), Bytes.toBytes(String.valueOf(part)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("offset"), Bytes.toBytes(String.valueOf(offset)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ifParseNormal"), Bytes.toBytes("1"))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("houseid"), Bytes.toBytes(houseid))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("gathertime"), Bytes.toBytes(gathertime))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("srcip"), Bytes.toBytes(srcip))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("destip"), Bytes.toBytes(destip))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("srcport"), Bytes.toBytes(srcport))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("desport"), Bytes.toBytes(desport))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("url"), Bytes.toBytes(url))
      put

    }catch {
      case e:Exception =>{
        e.printStackTrace()
        val md5pre = md5(topic + "|" + groupName + "|" + part)
        val rowkey = md5pre + "|" + String.format("%020d", java.lang.Long.valueOf(offset))
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("topic"), Bytes.toBytes(topic))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("part"), Bytes.toBytes(part))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("offset"), Bytes.toBytes(offset))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ifParseNormal"), Bytes.toBytes("0"))
        put
      }
    }

  }


  import java.io.UnsupportedEncodingException
  import java.security.MessageDigest
  import java.security.NoSuchAlgorithmException

  /**
    * 计算字符串的MD5值
    *
    * @param string 明文
    * @return 字符串的MD5值
    */
  def md5(string: String): String = {
    var md5:MessageDigest = null
    try {
      md5 = MessageDigest.getInstance("MD5")
      val bytes = md5.digest(string.getBytes("UTF-8"))
      var result = ""
      for (b <- bytes) {
        var temp = Integer.toHexString(b & 0xff)
        if (temp.length == 1) temp = "0" + temp
        result += temp
      }
      return result
    } catch {
      case e: NoSuchAlgorithmException =>
        e.printStackTrace()
      case e: UnsupportedEncodingException =>
        e.printStackTrace()
    }
    return "00000000000000000000000000000000"

  }

  def parseToHadoopDataSet(line:Tuple4[String, Int, Long, String], groupName:String) = {
    val topic = line._1
    val part = line._2
    val offset = line._3

    try{
      val msg = line._4.replaceAll("<<<!>>>", "")
      val arr = msg.split(",", 15)
      val houseid= arr(1)
      val gathertime = arr(2)
      val srcip = arr(3)
      val destip = arr(4)
      val srcport = arr(5)
      val desport = arr(6)
      val url = arr(13)
      val md5pre = md5(topic + "|" + groupName + "|" + part)

      val rowkey = md5pre + "|" + String.format("%020d", java.lang.Long.valueOf(offset))

      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ifParseNormal"), Bytes.toBytes("1"))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("houseid"), Bytes.toBytes(houseid))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("gathertime"), Bytes.toBytes(gathertime))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("srcip"), Bytes.toBytes(srcip))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("destip"), Bytes.toBytes(destip))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("srcport"), Bytes.toBytes(srcport))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("desport"), Bytes.toBytes(desport))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("url"), Bytes.toBytes(url))
      (new ImmutableBytesWritable , put)

    }catch {
      case e:Exception =>{
        e.printStackTrace()
        val md5pre = md5(topic + "|" + groupName + "|" + part)
        val rowkey = md5pre + "|" + String.format("%020d", java.lang.Long.valueOf(offset))
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ifParseNormal"), Bytes.toBytes("0"))
        (new ImmutableBytesWritable , put)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(md5("a"))
    //println(md5("a"))
   // println(md5("0cc175b9c0f1b6a831c399e269772661"))
  }
}
