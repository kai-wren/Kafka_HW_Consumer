import java.nio.charset.Charset
import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.Deserializer

object WordCountPerLineApp extends App {

  def createConsumer(): KafkaConsumer[String, String] ={
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "ScalaGroup")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "StringToMapDeserializer")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("input"))
    consumer
  }

  def createProducer(): KafkaProducer[String, String] ={
    val props:Properties = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    producer
  }

  def countWordsInString(str: String): String = {
    str.replace(".", "").replace(",", "").split(" ").map(_.toLowerCase)
      .groupBy(identity).mapValues((s) => s.length).mkString("|")
  }

  def produceMessages(str: String, producer: KafkaProducer[String, String]) = {
    val strList  = str.split("[|]")
    for (s <- strList) {
      val keyVal = s.split("[\\s->\\s]+")
      val record = new ProducerRecord[String, String]("output", keyVal.head, keyVal.tail.head)
      producer.send(record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = if(exception != null) {
          exception.printStackTrace();
        } else {
          println("Record sent: offset = " + metadata.offset() + " partition = " + metadata.partition())
        }
      })
    }

  }

  val consumer = createConsumer()
  val producer = createProducer()

  try {
    while (true) {
      val duration: Duration = Duration.ofMillis(100)
      val records = consumer.poll(duration)

      records.forEach((r) => {
        produceMessages(countWordsInString(r.value()), producer)
        println("Record received: offset = " + r.offset() + " partition = " + r.partition() + " key = " + r.key() + " value = " + r.value() )
      })
    }
  } finally consumer.close()



}

class StringToMapDeserializer extends Deserializer[String]{
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  def deserialize(topic: String, data: Array[Byte]): String = {
    new String(data, Charset.forName("UTF-8"))
  }

  override def close(): Unit = {}

}