package com.mgu.kafkaexamples

import java.util.Properties

import com.mgu.kafkaexamples.util.ContainerUtil

import scala.reflect.ClassTag

object Settings {

  lazy val kafkaHost: String = ContainerUtil getHostIp "docker_kafka_1" concat ":" concat 9092.toString

  //(implicit c: ClassTag[T])

  case class ProducerSettings[I, O](bootstrapServers: List[String] = List(kafkaHost),
                                                          val encoder: Encoder[I, O]
                                                          /*keySerializer: String = "org.apache.kafka.common.serialization.StringSerializer",
                                                          valueSerializer: String = "org.apache.kafka.common.serialization.StringSerializer"*/) {

    def toProperties: Properties = {
      val producerProperties = new Properties()
      producerProperties.put("bootstrap.servers", bootstrapServers mkString ",")
      producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      producerProperties.put("value.serializer", encoder.compatibleKafkaSerializer.getCanonicalName)
      //producerProperties.put("key.serializer", keySerializer)
      //producerProperties.put("value.serializer", valueSerializer)
      producerProperties
    }
  }

  case class ConsumerSettings[I : ClassTag, O : ClassTag](bootstrapServers: List[String] = List(kafkaHost),
                                    topics: List[String] = List("test"),
                                    groupId: String = "kafka-scala-group-1",
                                    /*keyDeserializer: String = "org.apache.kafka.common.serialization.StringDeserializer",
                                    valueDeserializer: String = "org.apache.kafka.common.serialization.StringDeserializer",*/
                                    enableAutoCommit: Boolean = true,
                                    autoCommitInterval: Int = 1000,
                                    sessionTimeout: Int = 30000,
                                    val decoder: Decoder[I, O]) {

    def toProperties: Properties = {
      val consumerProperties = new Properties()
      consumerProperties.put("bootstrap.servers", bootstrapServers mkString ",")
      consumerProperties.put("group.id", groupId)
      consumerProperties.put("enable.auto.commit", enableAutoCommit.toString)
      consumerProperties.put("auto.commit.interval.ms", autoCommitInterval.toString)
      consumerProperties.put("session.timeout.ms", sessionTimeout.toString)
      consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      //consumerProperties.put("value.deserializer", valueDeserializer)
      consumerProperties.put("value.deserializer", decoder.compatibleKafkaDeserializer.getCanonicalName)
      consumerProperties
    }
  }
}
