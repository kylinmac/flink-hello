//package com.mc.flink.job
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumerBase}
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//import org.apache.kafka.common.serialization.StringSerializer
//
//import java.util.Properties
//import scala.io.Source
//
//object KafkaJob {
//
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment()
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers","192.168.0.116:9092")
//    properties.setProperty("group.id","flinkHello")
//    properties.setProperty("key.serializer",classOf[StringSerializer].getName)
//    properties.setProperty("value.serializer",classOf[StringSerializer].getName)
//
//  }
//
//}
