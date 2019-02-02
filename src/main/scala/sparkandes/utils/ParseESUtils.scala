package sparkandes.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka.OffsetRange


/**
  *  解析日志
  */
object ParseESUtils {

  /**
    * @param line
    * @return
    */
  def parseMsg(line:Tuple4[String, Int, Long, String], offsetRanges: Array[OffsetRange], groupName:String) :Row = {
    val topic = line._1
    val part = line._2
    val offset = line._3

    val offsetRange = offsetRanges(part)
    val from = offsetRange.fromOffset
    val cnt = offsetRange.count()


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

      val ifParseNormal = 1 // 解析是否正常标志位
      val md5pre = md5(topic + "|" + groupName + "|" + part)

      val id = md5pre + "|" + String.format("%020d", java.lang.Long.valueOf(offset))

      Row(id, topic, part, from, offset, cnt, ifParseNormal, houseid, gathertime, srcip, destip, srcport, desport, url)


    }catch {
      case e:Exception =>{
        e.printStackTrace()
        val md5pre = md5(topic + "|" + groupName + "|" + part)
        val ifParseNormal = 0 // 解析是否正常标志位
        val id = md5pre + "|" + String.format("%020d", java.lang.Long.valueOf(offset))
        Row(id, topic, part, from, offset, cnt, ifParseNormal, "-1",  "-1",  "-1",  "-1",  "-1",  "-1",  "-1")
      }
    }

  }


  def parseMsgtoMap(line:Tuple4[String, Int, Long, String], offsetRanges: Array[OffsetRange], groupName:String)  = {
    val topic = line._1
    val part = line._2
    val offset = line._3

    val offsetRange = offsetRanges(part)
    val from = offsetRange.fromOffset
    val cnt = offsetRange.count()


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

      val ifParseNormal = 1 // 解析是否正常标志位
      val md5pre = md5(topic + "|" + groupName + "|" + part)

      val id = md5pre + "|" + String.format("%020d", java.lang.Long.valueOf(offset))

      Map("id"->id, "topic" -> topic, "part" -> part, "from"->from, "offset" -> offset, "cnt"->cnt,
        "ifParseNormal"->ifParseNormal, "houseid"->houseid, "gathertime"->gathertime, "srcip"->srcip,
        "destip"->destip, "srcport"->srcport, "desport"->desport, "url"->url)


    }catch {
      case e:Exception =>{
        e.printStackTrace()
        val md5pre = md5(topic + "|" + groupName + "|" + part)
        val ifParseNormal = 0 // 解析是否正常标志位
        val id = md5pre + "|" + String.format("%020d", java.lang.Long.valueOf(offset))
        Map("id"->id, "topic" -> topic, "part" -> part, "from"->from, "offset" -> offset, "cnt"->cnt,
          "ifParseNormal"->ifParseNormal, "houseid"->"-1", "gathertime"->"-1", "srcip"->"-1",
          "destip"->"-1", "srcport"->"-1", "desport"->"-1", "url"->"-1")
      }
    }

  }

  val struct = StructType(Array(
    StructField("id", StringType),
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("from", LongType),
    StructField("offset", LongType),

    StructField("cnt", LongType),
    StructField("ifParseNormal", IntegerType),
    StructField("houseid", StringType),
    StructField("gathertime", StringType),
    StructField("srcip", StringType),

    StructField("destip", StringType),
    StructField("srcport", StringType),
    StructField("destport", StringType),
    StructField("url", StringType)

  ))


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


  def main(args: Array[String]): Unit = {
    println(md5("a"))
    //println(md5("a"))
   // println(md5("0cc175b9c0f1b6a831c399e269772661"))
  }
}
