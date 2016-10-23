package com.mgu.kafkaexamples

import java.util.{Arrays, UUID}

import com.mgu.kafkaexamples.ConsumerWorker._
import com.mgu.kafkaexamples.Settings.ConsumerSettings
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters
import scala.reflect.ClassTag

class ConsumerWorker[I: ClassTag, O: ClassTag](val workerId: String = UUID.randomUUID.toString.substring(0, 7),
                                               val settings: ConsumerSettings[I, O]) extends Runnable {

  @volatile
  private var running = true

  private val decoder: Decoder[I, O] = settings.decoder

  private lazy val underlyingConsumer = new KafkaConsumer[String, O](settings.toProperties)

  override def run() {
    logger.info(s"[${workerId}] Initialized underlying Kafka consumer.")
    logger.info(s"[${workerId}] Attempting to subscribe to topics.")
    subscribeTo()
    while (running) {
      val records = toSeq(underlyingConsumer.poll(100).iterator())
      records.map(_.value()).map(decoder.decode(_)(decoder.manifest)).foreach(onMessage(_))
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

  private def toSeq(recordsIter: java.util.Iterator[ConsumerRecord[String, O]]): Seq[ConsumerRecord[String, O]] =
    JavaConverters.asScalaIteratorConverter(recordsIter).asScala.toSeq

  private def onMessage(payload: Option[I]) = payload match {
    case Some(value) => logger.info(s"[${workerId}] Received payload: ${value}")
    case None => logger.info(s"[${workerId}] Received an empty payload. Probably unable to deserialize it properly.")
  }

  def shutdown() = running = false
}

object ConsumerWorker {

  protected val logger: Logger = LoggerFactory getLogger getClass
}
