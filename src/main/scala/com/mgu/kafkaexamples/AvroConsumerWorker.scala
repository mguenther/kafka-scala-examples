package com.mgu.kafkaexamples

import java.util.{Arrays, UUID}

import com.mgu.kafkaexamples.AvroConsumerWorker._
import com.mgu.kafkaexamples.Settings.ConsumerSettings
import com.twitter.bijection.avro.SpecificAvroCodecs
import org.apache.avro.specific.SpecificRecordBase
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters
import scala.reflect.ClassTag

class AvroConsumerWorker[T <: SpecificRecordBase : ClassTag](val workerId: String = randomConsumerId(),
                                                             val settings: ConsumerSettings = ConsumerSettings()
                                                               .copy(valueDeserializer = "org.apache.kafka.common.serialization.ByteArrayDeserializer")) extends Runnable {

  @volatile
  private var running = true

  private lazy val underlyingConsumer = new KafkaConsumer[String, Array[Byte]](settings.toProperties)

  override def run() = {
    logger.info(s"[${workerId}] Initialized underlying Kafka consumer.")
    logger.info(s"[${workerId}] Attempting to subscribe to topics.")
    subscribeTo()
    while (running) {
      val records = toSeq(underlyingConsumer.poll(1000).iterator())
      if (records.size > 0) logger.info(s"[${workerId}] Found ${records.size} messages. Trying to deserialize them.")
      records.map(_.value()).map(deserialize).foreach(onMessage)
    }
  }

  private def subscribeTo() = settings.topics foreach {
    topic => {
      underlyingConsumer.subscribe(Arrays.asList(topic))
      logger.info(s"[${workerId}] Subscribed to topic ${topic}.")
    }
  }

  private def toSeq(recordsIter: java.util.Iterator[ConsumerRecord[String, Array[Byte]]]): Seq[ConsumerRecord[String, Array[Byte]]] =
    JavaConverters.asScalaIteratorConverter(recordsIter).asScala.toSeq

  private def deserialize(payload: Array[Byte]): Option[T] = {
    try {
      Some(SpecificAvroCodecs.toBinary[T].invert(payload).get)
    } catch {
      case ex: Exception =>
        logger.info(s"[${workerId}] Caught exception with message '${ex.getMessage}' while trying to deserialize.")
        None
    }
  }

  private def onMessage(payload: Option[T]) = payload match {
    case Some(message) => logger.info(s"[${workerId}] Received payload: ${message}")
    case None => logger.info(s"[${workerId}] Received an empty payload. Probably unable to deserialize it properly.")
  }

  def shutdown() = running = false
}

object AvroConsumerWorker {

  protected val logger: Logger = LoggerFactory getLogger getClass

  def randomConsumerId(): String = UUID.randomUUID.toString.substring(0, 7)
}
