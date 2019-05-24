package com.atguigu.dw.gamallcanal.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Author lzc
  * Date 2019/5/17 4:45 PM
  */
object MyKafkaSender {
    val props = new Properties()
    // Kafka服务端的主机名和端口号
    props.put("bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9093")
    // key序列化
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // value序列化
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    
    def sendToKafka(topic: String, content: String) = {
        producer.send(new ProducerRecord[String, String](topic, content))
    }
}
