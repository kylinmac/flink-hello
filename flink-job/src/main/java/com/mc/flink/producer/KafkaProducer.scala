//package com.mc.flink.producer
//
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//import org.apache.kafka.common.serialization.StringSerializer
//
//import java.util.Properties
//
//object KafkaProducer {
//  def main(args: Array[String]): Unit = {
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers","192.168.0.116:9092")
//    properties.setProperty("key.serializer",classOf[StringSerializer].getName)
//    properties.setProperty("value.serializer",classOf[StringSerializer].getName)
//
//    val producer = new KafkaProducer[String,String](properties)
//
//    for (i<- 1 to 100){
//      val builder = new StringBuilder
//      builder.append(i).append(",").append(i+1).append(",").append(i+2)
//      producer.send(new ProducerRecord[String,String]("TEST",i+"",builder.toString()))
//    }
//  }
//
//}
