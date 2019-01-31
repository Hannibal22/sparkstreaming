package kafka4

/**
  * KafkaProducer的lazy加载
  */

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import scala.collection.JavaConversions._

/**
  *  如果Class和Object同名， 那么称Class为Object的伴生类。
      可以类比java的static修饰符。 因为scala中是没有static修饰符， 那么Object下的方法和成员变量等都是静态， 可以直接调用，而不需要创建对象。
      简单的说：对于Class和Object下面都有apply方法， 如果调用Object下的Apply，使用类名()即可。
      如果调用Class下的Apply， 那么需要先创建一个类的对象， 然后使用对象名()调用， 如val ab = new MyKafkaProducer() ， 然后ab()就是调用
       Class下的Apply方法。
  */
class MyKafkaProducer[K,V](createProducer:()=>KafkaProducer[K, V])  extends Serializable{

  lazy val producer:KafkaProducer[K, V] = createProducer()

  def send(topic: String, key: K, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, key, value))

  def send(topic: String, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, value))

  def apply() = {}
}

object MyKafkaProducer{

  def apply[K, V](properties: java.util.Properties): MyKafkaProducer[K, V] = {
    val config = properties.toMap
    val createProducer = () => {
      val producer = new KafkaProducer[K, V](config)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new MyKafkaProducer(createProducer)
  }
}

