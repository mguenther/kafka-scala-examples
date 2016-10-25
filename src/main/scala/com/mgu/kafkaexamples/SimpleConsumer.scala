package com.mgu.kafkaexamples

import java.util.Properties

import com.mgu.kafkaexamples.util.ContainerUtil
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, KafkaConsumer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions
import scala.collection.JavaConversions.{seqAsJavaList}

object SimpleConsumer extends App {

  val logger = LoggerFactory getLogger getClass

  val props = new Properties
  val kafkaHost = ContainerUtil.getHostIp("docker_kafka_1")
  props.put("bootstrap.servers", s"${kafkaHost}:9092")
  props.put("group.id", "scala-rhein-main-group")
  props.put("enable.auto.commit", "true")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer: Consumer[String, String] = new KafkaConsumer[String, String](props)
  consumer.subscribe(seqAsJavaList(List("test")))
  while (true) {
    val records: ConsumerRecords[String, String] = consumer.poll(100)
    JavaConversions
      .asScalaIterator(records.iterator)
      .foreach(record => logger.info(s"offset=${record.offset}, key=${record.key}, value=${record.value}"))
  }
}
