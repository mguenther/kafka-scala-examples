package com.mgu.kafkaexamples

import java.util.{Arrays, Properties, UUID}

import com.mgu.kafkaexamples.ConsumerWorker._
import com.mgu.kafkaexamples.util.{ContainerUtil, JsonUtil}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters

class ConsumerWorker(val workerId: String = UUID.randomUUID.toString.substring(0, 7),
                     val settings: ConsumerSettings = ConsumerSettings()) extends Runnable {

  @volatile
  private var running = true

  private lazy val underlyingConsumer = new KafkaConsumer[String, String](settings.toProperties)

  override def run {
    logger.info(s"[${workerId}] Initialized underlying Kafka consumer.")
    logger.info(s"[${workerId}] Attempting to subscribe to topics.")
    subscribeTo()
    while (running) {
      val records = toSeq(underlyingConsumer.poll(100).iterator())
      records.map(_.value()).map(deserialize).foreach(onMessage)
    }
    logger.info(s"[${workerId}] Closing underlying Kafka consumer.")
    underlyingConsumer.close()
    logger.info(s"[${workerId}] Underlying Kafka consumer has been closed.")
  }

  private def subscribeTo() = settings.topics foreach {
    topic => {
      underlyingConsumer.subscribe(Arrays.asList(topic))
      logger.info(s"[${workerId}] Subscribed to topic ${topic}.")
    }
  }

  private def toSeq(recordsIter: java.util.Iterator[ConsumerRecord[String, String]]): Seq[ConsumerRecord[String, String]] =
    JavaConverters.asScalaIteratorConverter(recordsIter).asScala.toSeq

  private def deserialize(payload: String): Option[Message] = {
    try {
      Some(JsonUtil.fromJson[Message](payload))
    } catch {
      case ex: Exception =>
        None
    }
  }

  private def onMessage(payload: Option[Message]) = payload match {
    case Some(value) => logger.info(s"[${workerId}] Received payload: ${value}")
    case None => logger.info(s"[${workerId}] Received an empty payload. Probably unable to deserialize it properly.")
  }

  def shutdown() = running = false
}

object ConsumerWorker {

  protected val logger: Logger = LoggerFactory getLogger getClass

  private val kafkaHost: String = ContainerUtil.getHostIp("docker_kafka_1").concat(":").concat(9092.toString)

  case class ConsumerSettings(bootstrapServers: List[String] = List(kafkaHost),
                              topics: List[String] = List("test"),
                              groupId: String = "kafka-scala-group-1",
                              keyDeserializer: String = "org.apache.kafka.common.serialization.StringDeserializer",
                              valueDeserializer: String = "org.apache.kafka.common.serialization.StringDeserializer",
                              enableAutoCommit: Boolean = true,
                              autoCommitInterval: Int = 1000,
                              sessionTimeout: Int = 30000) {

    def toProperties: Properties = {
      val consumerProperties = new Properties()
      consumerProperties.put("bootstrap.servers", bootstrapServers mkString ",")
      consumerProperties.put("group.id", groupId)
      consumerProperties.put("enable.auto.commit", enableAutoCommit.toString)
      consumerProperties.put("auto.commit.interval.ms", autoCommitInterval.toString)
      consumerProperties.put("session.timeout.ms", sessionTimeout.toString)
      consumerProperties.put("key.deserializer", keyDeserializer)
      consumerProperties.put("value.deserializer", valueDeserializer)
      consumerProperties
    }
  }
}
