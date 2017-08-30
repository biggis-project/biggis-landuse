package biggis.landuse.spark.examples

import java.util.HashMap

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * Created by ak on 12.05.2017.
  */
object UtilsKafka extends LazyLogging {

  case class Topic(topic: String)

  var producer: KafkaProducer[String, String] = null
  var topic: Topic = null

  def initKafka(topic: String)(implicit brokers: String = "localhost:9092"): Unit = {
    this.topic = Topic(topic)
    initKafkaProducer(brokers)
  }

  def initKafkaProducer(brokers: String = "localhost:9092"): Unit = {
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producer = new KafkaProducer[String, String](props)
  }

  def send(str: String)(implicit topic: Topic = this.topic): Unit = {
    val message = new ProducerRecord[String, String](topic.topic, null, str)
    producer.send(message)
  }
}
