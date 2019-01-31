package kafka4

/**
  *  解析日志
  */
object ParseUtils {

  def parseMsg(msg:String) :String = {
    try{
      val newMsg = msg.replaceAll("<<<!>>>", "")
      val arr = newMsg.split(",", 15)
      val houseid= arr(1)
      val gathertime = arr(2)
      val srcip = arr(3)
      val destip = arr(4)
      val srcport = arr(5)
      val desport = arr(6)
      val url = arr(13)

      houseid + "|" + gathertime + "|" + srcip  + ":" + srcport + "|" + destip +":" + desport + "|" + url
    }catch {
      case e:Exception =>{
        "0"
      }
    }

  }
}
