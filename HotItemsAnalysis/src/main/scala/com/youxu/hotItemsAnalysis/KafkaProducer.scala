package com.youxu.hotItemsAnalysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer {

  def main(args: Array[String]): Unit = {
    write2Kafka("hotItems")
  }

  def write2Kafka(topic: String): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    //定义一个kafka producer
    val producer = new KafkaProducer[String, String](properties)
    //从文件中读取数据
    val bufferedSource = io.Source.fromFile("E:\\\\IdeaProjects\\\\UserBehaviorAnalysis\\\\HotItemsAnalysis\\\\src\\\\main\\\\resources\\\\UserBehavior.csv")
    bufferedSource.getLines().foreach( line => {
      producer.send(new ProducerRecord[String,String](topic,line))
    })
    producer.close()
  }
}
