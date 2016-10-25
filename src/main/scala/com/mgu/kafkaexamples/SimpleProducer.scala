package com.mgu.kafkaexamples

import java.util.Properties

import com.mgu.kafkaexamples.util.ContainerUtil
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, Producer}
import org.slf4j.{LoggerFactory, Logger}

object SimpleProducer extends App {

  val logger: Logger = LoggerFactory getLogger getClass

  val props: Properties = new Properties
  val kafkaHost = ContainerUtil.getHostIp("docker_kafka_1")
  println(kafkaHost)
  props.put("bootstrap.servers", s"${kafkaHost}:9092")
  props.put(
    "key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put(
    "value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")

  val producer: Producer[String, String] = new KafkaProducer[String, String](props)

  (1 to 100).foreach(i => {
    val key = i.toString
    val payload = i.toString
    val message = new ProducerRecord[String, String]("test", key, payload)
    producer.send(message)
  })

  producer.close()
}
