//
//import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
//
//import java.util.Properties
//import scala.collection.JavaConverters._
//import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
//
//import scala.concurrent.duration.Duration
//
//object SimpleProducer {
//  def main(args: Array[String]): Unit = {
//    // Check arguments length value
//
//    // Assign topicName to string variable
//
//    // create instance for properties to access producer configs
//    val props: Properties = new Properties()
//    val props1:Properties = new Properties()
//    // Assign localhost id
//    props.put("bootstrap.servers", "localhost:9092")
//
//
//    // Set acknowledgements for producer requests.
////    props.put("acks", "all")
//
//    // If the request fails, the producer can automatically retry,
////    props.put("retries", "0")
//
//    // Specify buffer size in config
////    props.put("batch.size", "16384")
//
//    // Reduce the no of requests less than 0
////    props.put("linger.ms", "1")
//
//    // The buffer.memory controls the total amount of memory available to the producer for buffering.
////    props.put("buffer.memory", "33554432")
//
//    props.put("key.serializer",
//      "org.apache.kafka.common.serialization.StringSerializer")
//    props1.put("key.deserializer",
//      "org.apache.kafka.common.deserialization.StringSerializer")
//
//    props.put("value.serializer",
//      "org.apache.kafka.common.serialization.StringSerializer")
//    props1.put("value.deserializer",
//      "org.apache.kafka.common.deserialization.StringSerializer")
//
//    val producer: Producer[String, String] = new KafkaProducer[String, String](props)
//
////    for (i <- 0 until 10) {
////      producer.send(new ProducerRecord[String, String]("trialTopic",
////        Integer.toString(i), Integer.toString(i)))
////      println("Message sent successfully")
////    }
////    producer.close()
//
//    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props1)
//    val nums: List[String] = List("trialTopic")
//
//    consumer.subscribe(Seq("trialTopic", "topic2").asJava)
//
//
//    val records: ConsumerRecords[String, String] = consumer.poll(1000)
//    while (true) {
//      val records: ConsumerRecords[String, String] = consumer.poll(1000)
//
//      // Process records
//      import scala.collection.JavaConverters._
//      records.asScala.foreach(record => {
//        println(s"Received record: ${record.key()} -> ${record.value()}")
//      })
//    }
//
//  }
//
//
//}